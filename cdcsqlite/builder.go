package cdcsqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ConflictPolicy decides what happens when an incoming datum disagrees with
// something already staged or published with the same key.
type ConflictPolicy int

const (
	// ConflictReject (default) makes any disagreement a hard error so a bad
	// stream can never silently overwrite authoritative data.
	ConflictReject ConflictPolicy = iota
	// ConflictLastWins lets the incoming datum overwrite the stored one.
	ConflictLastWins
)

// Config configures a Builder.
type Config struct {
	// Path is the directory holding the published target database and the
	// staging area. It is created if missing.
	Path string
	// SnapshotConflict governs duplicate snapshot rows whose payload differs
	// from the row already staged under the same primary key.
	SnapshotConflict ConflictPolicy
	// CDCConflict governs a re-sent CDC sequence number whose envelope differs
	// from the one already accepted.
	CDCConflict ConflictPolicy
}

// Sentinel errors. Callers can use errors.Is to branch on them.
var (
	// ErrNoRebuild is returned when an event can only be handled inside a
	// rebuild but none is active.
	ErrNoRebuild = errors.New("cdcsqlite: no rebuild in progress")
	// ErrRebuildInProgress is returned when an incremental-only event arrives
	// while a rebuild is still being staged.
	ErrRebuildInProgress = errors.New("cdcsqlite: rebuild in progress")
	// ErrGenerationMismatch is returned when an event belongs to a generation
	// other than the one currently being accepted. Old generations are never
	// mixed into a newer target.
	ErrGenerationMismatch = errors.New("cdcsqlite: event generation mismatch")
	// ErrGenerationRollback is returned by BeginRebuild when the requested
	// generation is not greater than the published generation.
	ErrGenerationRollback = errors.New("cdcsqlite: rebuild generation would roll back published target")
	// ErrDuplicate is returned when a re-sent key/sequence carries different
	// data and the matching conflict policy is ConflictReject.
	ErrDuplicate = errors.New("cdcsqlite: duplicate key with conflicting payload")
	// ErrOutOfOrder is returned when a CDC sequence is not the immediate
	// successor of the last accepted one.
	ErrOutOfOrder = errors.New("cdcsqlite: gap or out-of-order CDC sequence")
	// ErrNotPublishable is returned by TryPublish when the snapshot end
	// evidence or the complete (L,H] CDC prefix is missing.
	ErrNotPublishable = errors.New("cdcsqlite: snapshot end or CDC prefix incomplete")
)

const (
	targetDBFile    = "target.db"
	stagingDBFile   = "staging.db"
	stagingAttached = "stg"
	metadataTable   = "cdc_meta"
	targetTable     = "users"
)

// Builder ingests snapshot/CDC events and maintains one SQLite baseline.
// Writers serialize under an RWMutex while readers take the read lock on a
// separate WAL read connection, so a committing rebuild never blocks consumers
// and consumers only ever see fully committed snapshots.
type Builder struct {
	mu      *sync.RWMutex
	target  *sqliteDB // single long-lived writer connection
	reader  *sqliteDB // independent read-only connection (WAL snapshot isolation)
	staging *sqliteDB // nil unless a rebuild is active
	cfg     Config

	// Published state (target database).
	publishedGen int64
	watermark    int64 // H: highest CDC seq durably applied to the target

	// Active rebuild state (staging database cache of stg_rebuild_state).
	rbGen      int64
	rbCut      int64 // L
	cutPinned  bool  // distinguishes pinned L=0 from an unpinned cut
	rbH        int64 // highest buffered post-cut seq
	rbDone     bool
	rbRows     int64
	rbRowCount int64
	rbDigest   string
	rbDigestOK bool
	prefixNext int64
}

// Open opens (or creates) the baseline at cfg.Path and recovers any durable
// rebuild left behind by a previous process.
func Open(cfg Config) (*Builder, error) {
	if strings.TrimSpace(cfg.Path) == "" {
		return nil, errors.New("cdcsqlite: Config.Path is required")
	}
	if cfg.SnapshotConflict == 0 {
		cfg.SnapshotConflict = ConflictReject
	}
	if cfg.CDCConflict == 0 {
		cfg.CDCConflict = ConflictReject
	}
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("cdcsqlite: create data dir: %w", err)
	}

	b := &Builder{cfg: cfg, watermark: 0, prefixNext: 0}
	b.mu = &sync.RWMutex{}

	path := filepath.Join(cfg.Path, targetDBFile)
	db, err := openSQLite(path, sqliteOpenReadWrite)
	if err != nil {
		return nil, err
	}
	b.target = db
	// WAL keeps committed readers on the old snapshot while a writer commits.
	if err := db.exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.close()
		return nil, err
	}
	if err := db.exec("PRAGMA synchronous=FULL"); err != nil {
		_ = db.close()
		return nil, err
	}
	// The reader opens after WAL mode is established, so it sees committed
	// snapshots without ever taking a write lock.
	rdb, err := openSQLite(path, sqliteOpenReadOnly)
	if err != nil {
		_ = db.close()
		return nil, err
	}
	b.reader = rdb
	if err := db.ensureTargetSchema(); err != nil {
		_ = b.closeHandles()
		return nil, err
	}
	if err := b.loadPublished(); err != nil {
		_ = b.closeHandles()
		return nil, err
	}
	if err := b.recoverStaging(); err != nil {
		_ = b.closeHandles()
		return nil, err
	}
	return b, nil
}

func (b *Builder) closeHandles() error {
	if b.staging != nil {
		_ = b.staging.close()
		b.staging = nil
	}
	var first error
	if b.reader != nil {
		first = b.reader.close()
		b.reader = nil
	}
	if b.target != nil {
		if err := b.target.close(); err != nil && first == nil {
			first = err
		}
		b.target = nil
	}
	return first
}

func (db *sqliteDB) ensureTargetSchema() error {
	stmts := []string{
		"PRAGMA foreign_keys=ON",
		`CREATE TABLE IF NOT EXISTS ` + targetTable + ` (
			id     INTEGER PRIMARY KEY,
			email  TEXT,
			active INTEGER NOT NULL,
			score  REAL NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS ` + metadataTable + ` (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, s := range stmts {
		if err := db.exec(s); err != nil {
			return err
		}
	}
	return db.exec(`INSERT OR IGNORE INTO ` + metadataTable + `(key,value) VALUES
		('published_gen','0'),('watermark','0')`)
}

func (b *Builder) loadPublished() error {
	gen, err := b.metaGet("published_gen")
	if err != nil {
		return err
	}
	wm, err := b.metaGet("watermark")
	if err != nil {
		return err
	}
	b.publishedGen = parseInt64(gen)
	b.watermark = parseInt64(wm)
	return nil
}

// recoverStaging reopens an unfinished rebuild so a restarted process resumes
// staging instead of starting over. A staging file whose generation has
// already been published is discarded.
func (b *Builder) recoverStaging() error {
	stagingPath := filepath.Join(b.cfg.Path, stagingDBFile)
	if _, err := os.Stat(stagingPath); err != nil {
		if os.IsNotExist(err) {
			return nil // nothing to recover
		}
		return err
	}
	db, err := openSQLite(stagingPath, sqliteOpenReadWrite)
	if err != nil {
		// Unreadable staging is abandoned work, never the published target:
		// remove only staging artifacts so the live baseline survives.
		_ = removeStagingFiles(b.cfg.Path)
		return fmt.Errorf("cdcsqlite: open staging for recovery: %w", err)
	}
	if err := db.exec("PRAGMA synchronous=FULL"); err != nil {
		_ = db.close()
		return err
	}
	gen, err := db.scalarInt64("SELECT COALESCE((SELECT value FROM stg_rebuild_state WHERE key='gen'),'0')")
	if err != nil {
		_ = db.close()
		// Missing state table means a half-initialized file: discard safely,
		// again without touching the published target database.
		if removeErr := removeStagingFiles(b.cfg.Path); removeErr != nil {
			return removeErr
		}
		return nil
	}
	if gen <= b.publishedGen {
		// Already superseded by a publish (or never valid): old generation must
		// never mix into or roll back the target.
		_ = db.close()
		return removeStagingFiles(b.cfg.Path)
	}
	b.staging = db
	b.rbGen = gen
	if b.rbCut, err = db.scalarInt64("SELECT value FROM stg_rebuild_state WHERE key='cut'"); err != nil {
		return err
	}
	// A recovered staging file always carries an explicit cut row (0 is a
	// valid pinned cut), so the cut is considered pinned.
	b.cutPinned = true
	if b.rbH, err = db.scalarInt64("SELECT value FROM stg_rebuild_state WHERE key='h'"); err != nil {
		return err
	}
	if b.prefixNext, err = db.scalarInt64("SELECT value FROM stg_rebuild_state WHERE key='prefix_next'"); err != nil {
		return err
	}
	if b.rbRows, err = db.scalarInt64("SELECT value FROM stg_rebuild_state WHERE key='rows'"); err != nil {
		return err
	}
	if b.rbRowCount, err = db.scalarInt64("SELECT value FROM stg_rebuild_state WHERE key='row_count'"); err != nil {
		return err
	}
	done, err := db.scalarInt64("SELECT value FROM stg_rebuild_state WHERE key='done'")
	if err != nil {
		return err
	}
	b.rbDone = done == 1
	if b.rbDigest, b.rbDigestOK, err = db.scalarStringNullable(
		"SELECT value FROM stg_rebuild_state WHERE key='digest'"); err != nil {
		return err
	}
	return nil
}

// BeginRebuild starts (or resumes, when gen matches the active generation) a
// full rebuild. The target is never touched until TryPublish commits.
func (b *Builder) BeginRebuild(gen int64) error {
	if gen <= 0 {
		return errors.New("cdcsqlite: rebuild generation must be positive")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.staging != nil && b.rbGen == gen {
		return nil // idempotent resume of the same in-flight generation
	}
	if gen <= b.publishedGen {
		return fmt.Errorf("%w: requested %d <= published %d",
			ErrGenerationRollback, gen, b.publishedGen)
	}
	if b.staging != nil {
		// A newer rebuild request supersedes an unfinished older generation.
		_ = b.discardStagingLocked()
	}
	return b.startStagingLocked(gen)
}

func (b *Builder) startStagingLocked(gen int64) error {
	path := filepath.Join(b.cfg.Path, stagingDBFile)
	db, err := openSQLite(path, sqliteOpenReadWrite)
	if err != nil {
		return err
	}
	if err := db.exec("PRAGMA synchronous=FULL"); err != nil {
		_ = db.close()
		return err
	}
	stmts := []string{
		`CREATE TABLE stg_rebuild_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE stg_snapshot (
			id INTEGER PRIMARY KEY,
			email TEXT,
			active INTEGER NOT NULL,
			score REAL NOT NULL
		)`,
		`CREATE TABLE stg_cdc (
			seq INTEGER PRIMARY KEY,
			kind TEXT NOT NULL,
			id INTEGER,
			email TEXT,
			active INTEGER,
			score REAL
		)`,
		// Dedup ledger. Snapshot rows use the negative id space, CDC events
		// their positive seq, and the end marker key 0.
		`CREATE TABLE stg_seen (seen_key INTEGER PRIMARY KEY)`,
	}
	for _, s := range stmts {
		if err := db.exec(s); err != nil {
			_ = db.close()
			_ = os.Remove(path)
			return err
		}
	}
	b.staging = db
	b.rbGen = gen
	b.rbCut = 0
	b.cutPinned = false
	b.rbH = 0
	b.rbDone = false
	b.rbRows = 0
	b.rbRowCount = 0
	b.rbDigest = ""
	b.rbDigestOK = false
	// prefixNext is (re)computed when the first artifact pins cut L; until then
	// no post-cut event may be accepted.
	b.prefixNext = 0
	for _, kv := range [][2]string{
		{"gen", itoa(gen)}, {"cut", "0"}, {"h", "0"}, {"done", "0"},
		{"rows", "0"}, {"row_count", "0"}, {"prefix_next", "0"},
	} {
		if err := b.stgSet(kv[0], kv[1]); err != nil {
			_ = b.discardStagingLocked()
			return err
		}
	}
	return nil
}

func (b *Builder) discardStagingLocked() error {
	if b.staging != nil {
		_ = b.staging.close()
		b.staging = nil
	}
	if err := removeStagingFiles(b.cfg.Path); err != nil {
		return err
	}
	b.rbGen, b.rbCut, b.rbH = 0, 0, 0
	b.cutPinned = false
	b.rbDone, b.rbDigestOK = false, false
	b.rbRows, b.rbRowCount, b.prefixNext = 0, 0, 0
	return nil
}

// removeStagingFiles deletes only staging artifacts; the published target
// database is never removed by rebuild lifecycle management.
func removeStagingFiles(dir string) error {
	for _, name := range []string{stagingDBFile, stagingDBFile + "-wal", stagingDBFile + "-shm"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// ApplyResult reports how one Apply call was handled.
type ApplyResult struct {
	// Kind is the wire event kind.
	Kind EventKind
	// Seq is the event sequence (0 for snapshot artifacts).
	Seq int64
	// Staged is true when the event produced new durable staging/published
	// state; false when it was ignored as a duplicate or pre-cut re-delivery.
	Staged bool
	// Duplicate is true when an identical re-delivery was recognized and
	// skipped.
	Duplicate bool
	// PreCut is true when a rebuild CDC event was already reflected in the
	// snapshot (seq <= L) and therefore safely ignored.
	PreCut bool
	// Watermark is the durable high-watermark after the call. During a rebuild
	// it stays at the published value; it only advances inside a commit.
	Watermark int64
}

// Apply decodes one real Avro binary envelope and durably records it.
//
// Receiving bytes is never treated as persistence: nothing is acknowledged
// until the corresponding SQLite transaction has committed. A failure leaves
// both the target and the staging area exactly as they were before the call.
func (b *Builder) Apply(payload []byte) (*ApplyResult, error) {
	ev, err := DecodeEvent(payload)
	if err != nil {
		return nil, err
	}
	return b.ApplyEvent(ev)
}

// ApplyEvent is Apply with an already-decoded envelope.
func (b *Builder) ApplyEvent(ev *Event) (*ApplyResult, error) {
	if ev == nil {
		return nil, errors.New("cdcsqlite: nil event")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.staging != nil {
		return b.applyRebuildLocked(ev)
	}
	return b.applyIncrementalLocked(ev)
}

func (b *Builder) applyRebuildLocked(ev *Event) (*ApplyResult, error) {
	if ev.Gen != b.rbGen {
		return nil, fmt.Errorf("%w: event gen %d, rebuild gen %d",
			ErrGenerationMismatch, ev.Gen, b.rbGen)
	}
	if err := b.staging.exec("BEGIN IMMEDIATE"); err != nil {
		return nil, err
	}
	commit := false
	defer func() {
		if !commit {
			b.staging.rollback()
		}
	}()

	res := &ApplyResult{Kind: ev.Kind, Seq: ev.Seq, Watermark: b.watermark}
	var err error
	switch ev.Kind {
	case KindSnapshotRow:
		err = b.stageSnapshotRowLocked(ev, res)
	case KindSnapshotEnd:
		err = b.stageSnapshotEndLocked(ev, res)
	case KindUpsert, KindDelete:
		err = b.stageCDCLocked(ev, res)
	default:
		err = fmt.Errorf("cdcsqlite: unsupported event kind %q", ev.Kind)
	}
	if err != nil {
		return nil, err
	}
	if err := b.staging.exec("COMMIT"); err != nil {
		return nil, err
	}
	commit = true
	return res, nil
}

// pinCutLocked fixes cut L on the first artifact of a rebuild and rejects any
// later artifact carrying a different cut. Snapshot rows, buffered CDC events
// and the end marker all share the same cut. L=0 means the snapshot covers the
// population from the very beginning, so the first expected CDC seq is 1.
func (b *Builder) pinCutLocked(cut int64) error {
	if b.rbCut == 0 && !b.cutPinned {
		if cut < 0 {
			return errors.New("cdcsqlite: rebuild cut L must not be negative")
		}
		b.rbCut = cut
		b.cutPinned = true
		b.prefixNext = cut + 1
		if err := b.stgSet("cut", itoa(cut)); err != nil {
			return err
		}
		return b.stgSet("prefix_next", itoa(b.prefixNext))
	}
	if cut != b.rbCut {
		return fmt.Errorf("cdcsqlite: inconsistent rebuild cut: got %d want %d",
			cut, b.rbCut)
	}
	return nil
}

// stageSnapshotRowLocked inserts a snapshot row belonging to cut L.
func (b *Builder) stageSnapshotRowLocked(ev *Event, res *ApplyResult) error {
	if ev.Row == nil {
		return errors.New("cdcsqlite: snapshot row missing payload")
	}
	if b.rbDone {
		return errors.New("cdcsqlite: snapshot row after SNAPSHOT_END")
	}
	if err := b.pinCutLocked(ev.Cut); err != nil {
		return err
	}

	seenKey := -ev.Row.ID - 1 // negative id space, never collides with seq >= 0
	seen, err := b.stgSeen(seenKey)
	if err != nil {
		return err
	}
	if seen {
		existing, ok, err := b.stgSelectRow(ev.Row.ID)
		if err != nil {
			return err
		}
		if ok && rowsEqual(existing, ev.Row) {
			res.Staged, res.Duplicate = false, true
			return nil
		}
		if b.cfg.SnapshotConflict == ConflictReject {
			return fmt.Errorf("%w: snapshot id %d", ErrDuplicate, ev.Row.ID)
		}
	}

	if err := b.stgUpsertSnapshot(ev.Row); err != nil {
		return err
	}
	if err := b.stgInsertSeen(seenKey); err != nil {
		return err
	}
	if !seen {
		b.rbRows++
		if err := b.stgSet("rows", itoa(b.rbRows)); err != nil {
			return err
		}
	}
	res.Staged = true
	return nil
}

// stageCDCLocked buffers one ordered CDC event. Events with seq <= L are
// already part of the snapshot population and are explicitly skipped.
func (b *Builder) stageCDCLocked(ev *Event, res *ApplyResult) error {
	if err := b.pinCutLocked(ev.Cut); err != nil {
		return err
	}
	seen, err := b.stgSeen(ev.Seq)
	if err != nil {
		return err
	}
	if seen {
		existing, ok, err := b.stgSelectCDC(ev.Seq)
		if err != nil {
			return err
		}
		if ok && cdcEqual(existing, ev) {
			res.Staged, res.Duplicate = false, true
			return nil
		}
		if b.cfg.CDCConflict == ConflictReject {
			return fmt.Errorf("%w: cdc seq %d", ErrDuplicate, ev.Seq)
		}
	}

	if ev.Seq <= b.rbCut {
		// Authoritatively included in the snapshot; record acceptance only.
		if err := b.stgInsertSeen(ev.Seq); err != nil {
			return err
		}
		res.Staged, res.PreCut = false, true
		return nil
	}
	// Post-cut events must form the gapless prefix L+1, L+2, ..., H.
	if ev.Seq != b.prefixNext {
		return fmt.Errorf("%w: got seq %d, expected %d",
			ErrOutOfOrder, ev.Seq, b.prefixNext)
	}
	if err := b.stgInsertCDC(ev); err != nil {
		return err
	}
	if err := b.stgInsertSeen(ev.Seq); err != nil {
		return err
	}
	b.prefixNext++
	b.rbH = ev.Seq
	if err := b.stgSet("prefix_next", itoa(b.prefixNext)); err != nil {
		return err
	}
	if err := b.stgSet("h", itoa(b.rbH)); err != nil {
		return err
	}
	res.Staged = true
	return nil
}

// stageSnapshotEndLocked records the completeness evidence for the snapshot.
func (b *Builder) stageSnapshotEndLocked(ev *Event, res *ApplyResult) error {
	if err := b.pinCutLocked(ev.Cut); err != nil {
		return err
	}
	if b.rbDone {
		// Re-delivery of the identical end marker is an idempotent no-op.
		res.Staged, res.Duplicate = false, true
		return nil
	}
	if ev.RowCount != b.rbRows {
		return fmt.Errorf("cdcsqlite: snapshot end claims %d rows, %d staged",
			ev.RowCount, b.rbRows)
	}
	if ev.DigestPresent && b.rbDigestOK && ev.Digest != b.rbDigest {
		return fmt.Errorf("%w: snapshot digest", ErrDuplicate)
	}
	b.rbDone = true
	b.rbRowCount = ev.RowCount
	b.rbDigest = ev.Digest
	b.rbDigestOK = ev.DigestPresent
	for _, kv := range [][2]string{
		{"done", "1"},
		{"row_count", itoa(ev.RowCount)},
	} {
		if err := b.stgSet(kv[0], kv[1]); err != nil {
			return err
		}
	}
	if ev.DigestPresent {
		if err := b.stgSetDigest(ev.Digest); err != nil {
			return err
		}
	}
	res.Staged = true
	return nil
}

// PublishResult summarizes an atomic publish.
type PublishResult struct {
	Generation int64
	// Cut L of the snapshot and high-watermark H now published.
	Cut       int64
	Watermark int64
	Rows      int64
	Applied   int64 // CDC events replayed onto the new baseline
}

// TryPublish atomically swaps the rebuilt baseline into service. It succeeds
// only when (1) the SNAPSHOT_END completeness evidence is present and (2) the
// entire gapless CDC prefix (L, H] has been buffered. Until that moment
// consumers keep reading the previous, complete target untouched.
func (b *Builder) TryPublish() (*PublishResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.staging == nil {
		return nil, ErrNoRebuild
	}
	if !b.rbDone {
		return nil, fmt.Errorf("%w: missing SNAPSHOT_END", ErrNotPublishable)
	}
	if !b.cutPinned {
		return nil, fmt.Errorf("%w: snapshot cut never established", ErrNotPublishable)
	}
	// The buffered post-cut events must be exactly the gapless prefix
	// L+1 .. H with nothing missing.
	if b.prefixNext != b.rbH+1 || b.rbH < b.rbCut {
		return nil, fmt.Errorf("%w: prefix expected through %d, buffered through %d",
			ErrNotPublishable, b.prefixNext-1, b.rbH)
	}

	// The publish runs on the target connection with staging attached
	// read-mostly, so every change below is one cross-database transaction.
	attach := fmt.Sprintf("ATTACH DATABASE '%s' AS %s",
		escapeSQLitePath(filepath.Join(b.cfg.Path, stagingDBFile)), stagingAttached)
	if err := b.target.exec(attach); err != nil {
		return nil, err
	}
	defer func() { _ = b.target.exec("DETACH DATABASE " + stagingAttached) }()

	if err := b.target.exec("BEGIN IMMEDIATE"); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			b.target.rollback()
		}
	}()

	if err := b.target.exec("DROP TABLE IF EXISTS users_new"); err != nil {
		return nil, err
	}
	if err := b.target.exec(`CREATE TABLE users_new (
		id INTEGER PRIMARY KEY, email TEXT, active INTEGER NOT NULL, score REAL NOT NULL)`); err != nil {
		return nil, err
	}
	// An empty snapshot simply inserts zero rows, so the replacement
	// population is correctly empty.
	if err := b.target.exec(`INSERT INTO users_new(id,email,active,score)
		SELECT id,email,active,score FROM stg.stg_snapshot`); err != nil {
		return nil, err
	}
	applied, err := b.replayBufferedCDCLocked()
	if err != nil {
		return nil, err
	}
	// Table swap inside the transaction: the old complete table stays readable
	// to other connections right up to COMMIT.
	if err := b.target.exec("DROP TABLE IF EXISTS users_old"); err != nil {
		return nil, err
	}
	if err := b.target.exec("ALTER TABLE " + targetTable + " RENAME TO users_old"); err != nil {
		return nil, err
	}
	if err := b.target.exec("ALTER TABLE users_new RENAME TO " + targetTable); err != nil {
		return nil, err
	}
	if err := b.target.exec("DROP TABLE users_old"); err != nil {
		return nil, err
	}
	// Checkpoint advances in the SAME transaction as the new content: nobody
	// can observe H without also observing the matching rows.
	if err := b.metaSet("published_gen", itoa(b.rbGen)); err != nil {
		return nil, err
	}
	if err := b.metaSet("watermark", itoa(b.rbH)); err != nil {
		return nil, err
	}
	if err := b.target.exec("COMMIT"); err != nil {
		return nil, err
	}
	committed = true

	out := &PublishResult{
		Generation: b.rbGen,
		Cut:        b.rbCut,
		Watermark:  b.rbH,
		Rows:       b.rbRows,
		Applied:    applied,
	}
	// Staging is now superseded and must not survive to mix with later work.
	if err := b.discardStagingLocked(); err != nil {
		return out, err
	}
	b.publishedGen = out.Generation
	b.watermark = out.Watermark
	return out, nil
}

// replayBufferedCDCLocked applies buffered post-cut events in strict seq order
// against the freshly built users_new table.
func (b *Builder) replayBufferedCDCLocked() (int64, error) {
	rows, err := b.target.prepare(`SELECT seq,kind,id,email,active,score
		FROM stg.stg_cdc ORDER BY seq`)
	if err != nil {
		return 0, err
	}
	defer rows.finalize()

	upsert, err := b.target.prepare(`INSERT INTO users_new(id,email,active,score)
		VALUES(?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET email=excluded.email,
			active=excluded.active, score=excluded.score`)
	if err != nil {
		return 0, err
	}
	defer upsert.finalize()
	del, err := b.target.prepare("DELETE FROM users_new WHERE id=?")
	if err != nil {
		return 0, err
	}
	defer del.finalize()

	var applied, expectSeq int64 = 0, b.rbCut + 1
	for {
		done, err := rows.step()
		if err != nil {
			return applied, err
		}
		if done {
			break
		}
		seq := rows.columnInt64(0)
		if seq != expectSeq {
			return applied, fmt.Errorf("%w: buffered seq %d, expected %d",
				ErrOutOfOrder, seq, expectSeq)
		}
		kind := rows.columnText(1)
		id := rows.columnInt64(2)
		switch EventKind(kind) {
		case KindUpsert:
			upsert.reset()
			if err := upsert.bindInt64(1, id); err != nil {
				return applied, err
			}
			if rows.columnType(3) == sqliteNull {
				if err := upsert.bindNull(2); err != nil {
					return applied, err
				}
			} else if err := upsert.bindText(2, rows.columnText(3)); err != nil {
				return applied, err
			}
			if err := upsert.bindInt64(3, rows.columnInt64(4)); err != nil {
				return applied, err
			}
			if err := upsert.bindDouble(4, rows.columnDouble(5)); err != nil {
				return applied, err
			}
			if d, err := upsert.step(); err != nil || !d {
				if err != nil {
					return applied, err
				}
			}
		case KindDelete:
			del.reset()
			if err := del.bindInt64(1, id); err != nil {
				return applied, err
			}
			if d, err := del.step(); err != nil || !d {
				if err != nil {
					return applied, err
				}
			}
		default:
			return applied, fmt.Errorf("cdcsqlite: buffered unknown kind %q", kind)
		}
		applied++
		expectSeq++
	}
	if expectSeq != b.rbH+1 {
		return applied, fmt.Errorf("%w: replay reached %d, expected %d",
			ErrOutOfOrder, expectSeq-1, b.rbH)
	}
	return applied, nil
}

// applyIncrementalLocked handles ordered CDC after a baseline is published.
// Each accepted event and the new watermark commit in one transaction, so
// acknowledgement never outruns durable application.
func (b *Builder) applyIncrementalLocked(ev *Event) (*ApplyResult, error) {
	switch ev.Kind {
	case KindSnapshotRow, KindSnapshotEnd:
		return nil, fmt.Errorf("%w: snapshot artifact outside rebuild", ErrNoRebuild)
	}
	// Only the current generation (or legacy gen 0) may continue the target.
	if ev.Gen != 0 && ev.Gen != b.publishedGen {
		return nil, fmt.Errorf("%w: event gen %d, published gen %d",
			ErrGenerationMismatch, ev.Gen, b.publishedGen)
	}
	if ev.Seq <= b.watermark {
		// Re-delivery of already-published work. Verify ordering boundary only;
		// the target already contains its effect.
		return &ApplyResult{
			Kind: ev.Kind, Seq: ev.Seq, Duplicate: true, Watermark: b.watermark,
		}, nil
	}
	if ev.Seq != b.watermark+1 {
		return nil, fmt.Errorf("%w: got seq %d, expected %d",
			ErrOutOfOrder, ev.Seq, b.watermark+1)
	}

	if err := b.target.exec("BEGIN IMMEDIATE"); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			b.target.rollback()
		}
	}()
	if ev.Kind == KindUpsert {
		if err := b.targetUpsert(ev.Row); err != nil {
			return nil, err
		}
	} else {
		if err := b.targetDelete(ev.Row.ID); err != nil {
			return nil, err
		}
	}
	if err := b.metaSet("watermark", itoa(ev.Seq)); err != nil {
		return nil, err
	}
	if err := b.target.exec("COMMIT"); err != nil {
		return nil, err
	}
	committed = true
	b.watermark = ev.Seq
	return &ApplyResult{
		Kind: ev.Kind, Seq: ev.Seq, Staged: true, Watermark: b.watermark,
	}, nil
}

// AbortRebuild discards all staging for the active rebuild. The published
// target and checkpoint are untouched.
func (b *Builder) AbortRebuild() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.staging == nil {
		return ErrNoRebuild
	}
	return b.discardStagingLocked()
}

// Close releases all database handles. It is safe to call more than once.
func (b *Builder) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.target == nil && b.reader == nil && b.staging == nil {
		return nil
	}
	if b.target != nil {
		_ = b.target.exec("PRAGMA wal_checkpoint(TRUNCATE)")
	}
	return b.closeHandles()
}

// State is the publicly visible baseline state.
type State struct {
	PublishedGeneration int64
	// Watermark is the highest CDC sequence durably reflected in the target.
	Watermark int64
	// Rebuilding reports whether a rebuild is currently staging.
	Rebuilding bool
	// RebuildGeneration / Cut / BufferedH describe the in-flight rebuild.
	RebuildGeneration int64
	Cut               int64
	BufferedH         int64
	SnapshotDone      bool
	StagedRows        int64
}

// Snapshot returns the current durable state.
func (b *Builder) Snapshot() *State {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &State{
		PublishedGeneration: b.publishedGen,
		Watermark:           b.watermark,
	}
	if b.staging != nil {
		s.Rebuilding = true
		s.RebuildGeneration = b.rbGen
		s.Cut = b.rbCut
		s.BufferedH = b.rbH
		s.SnapshotDone = b.rbDone
		s.StagedRows = b.rbRows
	}
	return s
}

// GetRow reads one row from the published target by primary key.
func (b *Builder) GetRow(ctx context.Context, id int64) (*Row, bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_ = ctx
	st, err := b.reader.prepare("SELECT id,email,active,score FROM " +
		targetTable + " WHERE id=?")
	if err != nil {
		return nil, false, err
	}
	defer st.finalize()
	if err := st.bindInt64(1, id); err != nil {
		return nil, false, err
	}
	done, err := st.step()
	if err != nil {
		return nil, false, err
	}
	if done {
		return nil, false, nil
	}
	return scanRow(st), true, nil
}

// RowCount returns the number of rows in the published target.
func (b *Builder) RowCount(ctx context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_ = ctx
	return b.reader.scalarInt64("SELECT COUNT(*) FROM " + targetTable)
}

// Query runs fn for every target row in ascending primary-key order. Returning
// an error from fn stops the scan and propagates it.
func (b *Builder) Query(ctx context.Context, fn func(*Row) error) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_ = ctx
	st, err := b.reader.prepare("SELECT id,email,active,score FROM " +
		targetTable + " ORDER BY id")
	if err != nil {
		return err
	}
	defer st.finalize()
	for {
		done, err := st.step()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if err := fn(scanRow(st)); err != nil {
			return err
		}
	}
}

func scanRow(st *stmt) *Row {
	r := &Row{ID: st.columnInt64(0), Active: st.columnInt64(2) == 1, Score: st.columnDouble(3)}
	if st.columnType(1) != sqliteNull {
		r.Email = st.columnText(1)
		r.EmailPresent = true
	}
	return r
}

func rowsEqual(a, b *Row) bool {
	return a.ID == b.ID &&
		a.Active == b.Active &&
		a.Score == b.Score &&
		a.EmailPresent == b.EmailPresent &&
		(!a.EmailPresent || a.Email == b.Email)
}

// cdcEqual compares the event-relevant payload of a buffered CDC event.
func cdcEqual(stored stgCDCEvent, ev *Event) bool {
	if stored.Kind != ev.Kind {
		return false
	}
	if ev.Kind == KindDelete {
		return stored.ID == ev.Row.ID
	}
	return stored.ID == ev.Row.ID &&
		stored.Active == ev.Row.Active &&
		stored.Score == ev.Row.Score &&
		stored.EmailPresent == ev.Row.EmailPresent &&
		(!stored.EmailPresent || stored.Email == ev.Row.Email)
}

type stgCDCEvent struct {
	Seq          int64
	Kind         EventKind
	ID           int64
	Email        string
	EmailPresent bool
	Active       bool
	Score        float64
}

func escapeSQLitePath(p string) string {
	return strings.ReplaceAll(p, "'", "''")
}
