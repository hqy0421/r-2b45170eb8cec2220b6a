package cdcsqlite

import (
	"errors"
	"fmt"

	goavro "github.com/linkedin/goavro/v2"
)

// WireSchema is the single, fixed Avro schema governing every snapshot and CDC
// message ever accepted by this package. It never changes at runtime: a message
// that does not decode under this schema is rejected, so no ad-hoc schema can
// ever reach the baseline.
//
// The event envelope carries:
//   - gen:   snapshot generation (代次). Generation 0 means a plain CDC event
//     not tied to any rebuild; every rebuild generation must be strictly
//     greater than the last published one.
//   - kind:  one of SNAPSHOT_ROW / SNAPSHOT_END / UPSERT / DELETE.
//   - seq:   monotonic, gapless CDC log sequence number. Snapshot rows carry
//     seq=0; real CDC events carry seq > 0.
//   - cut:   the snapshot watermark L: the snapshot is consistent as of CDC
//     position L. Events with seq <= L are already reflected in the snapshot.
//   - row_count: number of snapshot rows claimed by the producer (0 when
//     unknown). SNAPSHOT_END records the definitive value; 0 here means an
//     intentionally empty population.
//   - digest: optional producer-supplied digest of the snapshot, compared for
//     equality between the end marker and the actually staged row count only
//     when both are present.
//   - row:   the fixed-schema User row (absent for SNAPSHOT_END and DELETE).
const WireSchema = `{
  "type": "record",
  "name": "Envelope",
  "namespace": "cdcsqlite",
  "fields": [
    {"name": "gen", "type": "long"},
    {"name": "kind", "type": {
      "type": "enum",
      "name": "EventKind",
      "symbols": ["SNAPSHOT_ROW", "SNAPSHOT_END", "UPSERT", "DELETE"]
    }},
    {"name": "seq", "type": "long"},
    {"name": "cut", "type": "long"},
    {"name": "row_count", "type": "long", "default": 0},
    {"name": "digest", "type": ["null", "string"], "default": null},
    {"name": "row", "type": ["null", {
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "id", "type": "long"},
        {"name": "email", "type": ["null", "string"], "default": null},
        {"name": "active", "type": "boolean", "default": true},
        {"name": "score", "type": "double", "default": 0.0}
      ]
    }], "default": null}
  ]
}`

// EventKind enumerates the wire-level event types.
type EventKind string

const (
	KindSnapshotRow EventKind = "SNAPSHOT_ROW"
	KindSnapshotEnd EventKind = "SNAPSHOT_END"
	KindUpsert      EventKind = "UPSERT"
	KindDelete      EventKind = "DELETE"
)

// Row is one fixed-schema population record. The primary key is always ID and
// its column is part of the fixed target schema.
type Row struct {
	ID     int64
	Email  string
	Active bool
	Score  float64
	// EmailPresent distinguishes a SQL NULL from an empty string.
	EmailPresent bool
}

// Event is one decoded Avro envelope.
type Event struct {
	Gen      int64
	Kind     EventKind
	Seq      int64
	Cut      int64
	RowCount int64
	Digest   string
	// DigestPresent distinguishes an absent digest from an empty one.
	DigestPresent bool
	Row           *Row
}

var envelopeCodec = mustNewCodec(WireSchema)

func mustNewCodec(schema string) *goavro.Codec {
	c, err := goavro.NewCodec(schema)
	if err != nil {
		panic(fmt.Sprintf("cdcsqlite: invalid fixed wire schema: %v", err))
	}
	return c
}

// DecodeEvent performs a real Avro binary decode under the fixed WireSchema.
// Trailing bytes are rejected: one call must consume exactly one envelope, so
// a truncated or concatenated payload can never be silently accepted.
func DecodeEvent(payload []byte) (*Event, error) {
	if len(payload) == 0 {
		return nil, errors.New("cdcsqlite: empty event payload")
	}
	native, remaining, err := envelopeCodec.NativeFromBinary(payload)
	if err != nil {
		return nil, fmt.Errorf("cdcsqlite: decode envelope: %w", err)
	}
	if len(remaining) != 0 {
		return nil, fmt.Errorf("cdcsqlite: %d trailing bytes after envelope", len(remaining))
	}
	return eventFromNative(native)
}

// EncodeEvent encodes an event under the fixed schema. It exists primarily so
// tests and producers can round-trip real Avro bytes through DecodeEvent.
func EncodeEvent(ev *Event) ([]byte, error) {
	if ev == nil {
		return nil, errors.New("cdcsqlite: nil event")
	}
	native, err := eventToNative(ev)
	if err != nil {
		return nil, err
	}
	buf, err := envelopeCodec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("cdcsqlite: encode envelope: %w", err)
	}
	return buf, nil
}

func eventFromNative(native interface{}) (*Event, error) {
	root, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.New("cdcsqlite: envelope is not a record")
	}
	ev := &Event{}
	var err error

	if ev.Gen, err = asInt64(root, "gen"); err != nil {
		return nil, err
	}
	kindStr, err := asString(root, "kind")
	if err != nil {
		return nil, err
	}
	switch kindStr {
	case string(KindSnapshotRow), string(KindSnapshotEnd),
		string(KindUpsert), string(KindDelete):
		ev.Kind = EventKind(kindStr)
	default:
		return nil, fmt.Errorf("cdcsqlite: unknown event kind %q", kindStr)
	}
	if ev.Seq, err = asInt64(root, "seq"); err != nil {
		return nil, err
	}
	if ev.Cut, err = asInt64(root, "cut"); err != nil {
		return nil, err
	}
	if ev.RowCount, err = asInt64(root, "row_count"); err != nil {
		return nil, err
	}
	if ev.Digest, ev.DigestPresent, err = asNullableString(root, "digest"); err != nil {
		return nil, err
	}

	rowUnion, ok := root["row"].(map[string]interface{})
	if ok {
		rowNative, ok := rowUnion["cdcsqlite.User"].(map[string]interface{})
		if !ok {
			return nil, errors.New("cdcsqlite: row union has unexpected member")
		}
		if ev.Row, err = rowFromNative(rowNative); err != nil {
			return nil, err
		}
	}
	if err := validateEvent(ev); err != nil {
		return nil, err
	}
	return ev, nil
}

func rowFromNative(m map[string]interface{}) (*Row, error) {
	r := &Row{}
	var err error
	if r.ID, err = asInt64(m, "id"); err != nil {
		return nil, err
	}
	if r.Email, r.EmailPresent, err = asNullableString(m, "email"); err != nil {
		return nil, err
	}
	active, err := asBool(m, "active")
	if err != nil {
		return nil, err
	}
	r.Active = active
	score, err := asFloat64(m, "score")
	if err != nil {
		return nil, err
	}
	r.Score = score
	return r, nil
}

func validateEvent(ev *Event) error {
	if ev.Gen < 0 || ev.Seq < 0 || ev.Cut < 0 || ev.RowCount < 0 {
		return errors.New("cdcsqlite: negative long field in envelope")
	}
	switch ev.Kind {
	case KindSnapshotRow:
		if ev.Row == nil {
			return errors.New("cdcsqlite: SNAPSHOT_ROW without row payload")
		}
	case KindSnapshotEnd:
		if ev.Row != nil {
			return errors.New("cdcsqlite: SNAPSHOT_END must not carry a row")
		}
	case KindUpsert:
		if ev.Seq == 0 {
			return errors.New("cdcsqlite: CDC event requires seq > 0")
		}
		if ev.Row == nil {
			return errors.New("cdcsqlite: UPSERT without row payload")
		}
	case KindDelete:
		if ev.Seq == 0 {
			return errors.New("cdcsqlite: CDC event requires seq > 0")
		}
		if ev.Row == nil {
			return errors.New("cdcsqlite: DELETE requires an id-only row payload")
		}
		if ev.Row.EmailPresent || ev.Row.Active || ev.Row.Score != 0 {
			return errors.New("cdcsqlite: DELETE row payload must carry only the id")
		}
	}
	return nil
}

func eventToNative(ev *Event) (map[string]interface{}, error) {
	if err := validateEvent(ev); err != nil {
		return nil, err
	}
	m := map[string]interface{}{
		"gen":       ev.Gen,
		"kind":      string(ev.Kind),
		"seq":       ev.Seq,
		"cut":       ev.Cut,
		"row_count": ev.RowCount,
	}
	if ev.DigestPresent {
		m["digest"] = map[string]interface{}{"string": ev.Digest}
	} else {
		m["digest"] = nil
	}
	if ev.Row != nil {
		m["row"] = map[string]interface{}{"cdcsqlite.User": rowToNative(ev.Row)}
	} else {
		m["row"] = nil
	}
	return m, nil
}

func rowToNative(r *Row) map[string]interface{} {
	m := map[string]interface{}{
		"id":     r.ID,
		"active": r.Active,
		"score":  r.Score,
	}
	if r.EmailPresent {
		m["email"] = map[string]interface{}{"string": r.Email}
	} else {
		m["email"] = nil
	}
	return m
}

func asInt64(m map[string]interface{}, key string) (int64, error) {
	v, ok := m[key]
	if !ok {
		return 0, fmt.Errorf("cdcsqlite: missing field %q", key)
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("cdcsqlite: field %q is %T, want long", key, v)
	}
	return n, nil
}

func asString(m map[string]interface{}, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", fmt.Errorf("cdcsqlite: missing field %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("cdcsqlite: field %q is %T, want string/enum", key, v)
	}
	return s, nil
}

func asBool(m map[string]interface{}, key string) (bool, error) {
	v, ok := m[key]
	if !ok {
		return false, fmt.Errorf("cdcsqlite: missing field %q", key)
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("cdcsqlite: field %q is %T, want boolean", key, v)
	}
	return b, nil
}

func asFloat64(m map[string]interface{}, key string) (float64, error) {
	v, ok := m[key]
	if !ok {
		return 0, fmt.Errorf("cdcsqlite: missing field %q", key)
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("cdcsqlite: field %q is %T, want double", key, v)
	}
	return f, nil
}

// asNullableString reads a ["null","string"] union.
func asNullableString(m map[string]interface{}, key string) (string, bool, error) {
	v, ok := m[key]
	if !ok {
		return "", false, fmt.Errorf("cdcsqlite: missing field %q", key)
	}
	if v == nil {
		return "", false, nil
	}
	u, ok := v.(map[string]interface{})
	if !ok {
		return "", false, fmt.Errorf("cdcsqlite: field %q is %T, want nullable union", key, v)
	}
	s, ok := u["string"].(string)
	if !ok {
		return "", false, fmt.Errorf("cdcsqlite: field %q union has no string member", key)
	}
	return s, true, nil
}
