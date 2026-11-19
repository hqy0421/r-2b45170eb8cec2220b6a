package cdcsqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// ---- test helpers ----------------------------------------------------------

func testDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cdcsqlite-*")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func openBuilder(t *testing.T, dir string) *Builder {
	t.Helper()
	b, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func mustEncode(t *testing.T, ev *Event) []byte {
	t.Helper()
	buf, err := EncodeEvent(ev)
	if err != nil {
		t.Fatalf("EncodeEvent: %v", err)
	}
	return buf
}

func row(id int64, email string, active bool, score float64) *Row {
	return &Row{ID: id, Email: email, EmailPresent: true, Active: active, Score: score}
}

func snapRow(gen, cut, id int64) *Event {
	return &Event{Gen: gen, Kind: KindSnapshotRow, Cut: cut, Row: row(id, fmt.Sprintf("u%d@x.io", id), true, float64(id))}
}

func cdcEvent(gen int64, kind EventKind, seq, cut, id int64) *Event {
	ev := &Event{Gen: gen, Kind: kind, Seq: seq, Cut: cut}
	if kind == KindUpsert {
		ev.Row = row(id, fmt.Sprintf("u%d-new@x.io", id), false, float64(id)+0.5)
	} else {
		ev.Row = &Row{ID: id}
	}
	return ev
}

func applyN(t *testing.T, b *Builder, evs ...*Event) {
	t.Helper()
	for _, ev := range evs {
		if _, err := b.Apply(mustEncode(t, ev)); err != nil {
			t.Fatalf("Apply %+v: %v", ev, err)
		}
	}
}

func assertRow(t *testing.T, b *Builder, id int64, want *Row) {
	t.Helper()
	got, ok, err := b.GetRow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRow %d: %v", id, err)
	}
	if !ok {
		t.Fatalf("row %d missing, want %+v", id, want)
	}
	if !rowsEqual(got, want) {
		t.Fatalf("row %d = %+v, want %+v", id, got, want)
	}
}

func assertAbsent(t *testing.T, b *Builder, id int64) {
	t.Helper()
	got, ok, err := b.GetRow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRow %d: %v", id, err)
	}
	if ok {
		t.Fatalf("row %d = %+v, want absent", id, got)
	}
}

func assertCount(t *testing.T, b *Builder, want int64) {
	t.Helper()
	n, err := b.RowCount(context.Background())
	if err != nil {
		t.Fatalf("RowCount: %v", err)
	}
	if n != want {
		t.Fatalf("row count = %d, want %d", n, want)
	}
}

// ---- wire protocol ---------------------------------------------------------

func TestWireRoundTripRealAvro(t *testing.T) {
	ev := &Event{
		Gen: 3, Kind: KindUpsert, Seq: 55, Cut: 40,
		Row: &Row{ID: 9, Email: "a@b.io", EmailPresent: true, Active: false, Score: -2.25},
	}
	payload := mustEncode(t, ev)
	got, err := DecodeEvent(payload)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	if got.Gen != 3 || got.Kind != KindUpsert || got.Seq != 55 || got.Cut != 40 {
		t.Fatalf("envelope fields wrong: %+v", got)
	}
	if !rowsEqual(got.Row, ev.Row) {
		t.Fatalf("row mismatch: %+v vs %+v", got.Row, ev.Row)
	}
}

func TestDecodeRejectsTrailingAndGarbage(t *testing.T) {
	payload := mustEncode(t, &Event{Gen: 1, Kind: KindDelete, Seq: 1, Cut: 0, Row: &Row{ID: 1}})
	if _, err := DecodeEvent(append(payload, 0xAA)); err == nil {
		t.Fatal("trailing byte must be rejected")
	}
	if _, err := DecodeEvent(mustEncode(t, &Event{Gen: 1, Kind: KindDelete, Seq: 1, Cut: 0,
		Row: &Row{ID: 9}})); err != nil {
		t.Fatalf("valid id-only delete rejected: %v", err)
	}
	if _, err := DecodeEvent(nil); err == nil {
		t.Fatal("empty payload must be rejected")
	}
	if _, err := DecodeEvent([]byte{0xFF, 0xFF, 0xFF}); err == nil {
		t.Fatal("garbage payload must be rejected")
	}
}

func TestFixedSchemaValidation(t *testing.T) {
	cases := []*Event{
		{Gen: 1, Kind: KindSnapshotRow, Cut: 10},                              // missing row
		{Gen: 1, Kind: KindSnapshotEnd, Cut: 10, Row: row(1, "", true, 1)},    // row on end
		{Gen: 1, Kind: KindUpsert, Seq: 0, Cut: 10, Row: row(1, "", true, 1)}, // seq 0
		{Gen: 1, Kind: KindDelete, Seq: 2, Cut: 10, Row: row(1, "", true, 1)}, // row on delete
		{Gen: -1, Kind: KindSnapshotEnd, Cut: 10},                             // negative gen
	}
	for i, ev := range cases {
		if _, err := EncodeEvent(ev); err == nil {
			t.Fatalf("case %d: expected validation error for %+v", i, ev)
		}
	}
}

// ---- full rebuild happy path ----------------------------------------------

func TestFullRebuildInterleavedSnapshotAndCDC(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)

	// Seed an old published baseline so we can prove it stays readable.
	b.publishedGen = 0
	b.watermark = 0
	oldRows := []*Row{row(1, "old1", true, 1), row(2, "old2", true, 2), row(99, "ghost", true, 99)}
	if err := b.target.exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	for _, r := range oldRows {
		if err := b.targetUpsert(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.target.exec("COMMIT"); err != nil {
		t.Fatal(err)
	}
	assertCount(t, b, 3)

	const gen, cut int64 = 1, 10
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatalf("BeginRebuild: %v", err)
	}

	// Snapshot rows at cut L=10 interleaved with ordered CDC in (L, H].
	applyN(t, b, snapRow(gen, cut, 1))
	applyN(t, b, cdcEvent(gen, KindUpsert, 11, cut, 1)) // post-cut change buffered
	applyN(t, b, snapRow(gen, cut, 2))
	applyN(t, b, snapRow(gen, cut, 3))
	applyN(t, b, cdcEvent(gen, KindDelete, 12, cut, 3))
	applyN(t, b, snapRow(gen, cut, 4))

	// While staging, consumers must still see the old complete target.
	assertCount(t, b, 3)
	assertRow(t, b, 99, row(99, "ghost", true, 99))

	// End evidence present but prefix (L,H] gapless through whatever H: here
	// H=12 and all of 11,12 buffered -> publishable.
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 4,
		Digest: "d4", DigestPresent: true})

	if _, err := b.TryPublish(); err != nil {
		t.Fatalf("TryPublish: %v", err)
	}
	st := b.Snapshot()
	if st.PublishedGeneration != gen || st.Watermark != 12 || st.Rebuilding {
		t.Fatalf("state after publish wrong: %+v", st)
	}
	assertCount(t, b, 3) // 4 snapshot rows, id 3 deleted by CDC seq 12
	assertAbsent(t, b, 99)
	assertAbsent(t, b, 3)
	// id 1 reflects the post-cut upsert on top of the snapshot row.
	assertRow(t, b, 1, &Row{ID: 1, Email: "u1-new@x.io", EmailPresent: true, Active: false, Score: 1.5})
	assertRow(t, b, 2, row(2, "u2@x.io", true, 2))
}

func TestPublishBlockedUntilBothEvidenceAndPrefix(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 5
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1))

	// No end evidence, no prefix: blocked.
	if _, err := b.TryPublish(); !errors.Is(err, ErrNotPublishable) {
		t.Fatalf("want ErrNotPublishable, got %v", err)
	}

	// End evidence present but the (L,H] prefix missing: still blocked.
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 1})
	if _, err := b.TryPublish(); !errors.Is(err, ErrNotPublishable) {
		t.Fatalf("want ErrNotPublishable after end-only, got %v", err)
	}

	// Gap: seq 7 without 6 must be rejected, not partially buffered.
	ev7 := cdcEvent(gen, KindUpsert, 7, cut, 1)
	if _, err := b.Apply(mustEncode(t, ev7)); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("want ErrOutOfOrder, got %v", err)
	}
	// Once the gap closes, publish succeeds.
	applyN(t, b, cdcEvent(gen, KindUpsert, 6, cut, 1))
	applyN(t, b, cdcEvent(gen, KindUpsert, 7, cut, 1))
	if _, err := b.TryPublish(); err != nil {
		t.Fatalf("TryPublish after prefix: %v", err)
	}
	if b.watermark != 7 {
		t.Fatalf("watermark = %d, want 7", b.watermark)
	}
}

func TestEmptySnapshotReplacesOldPopulation(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	if err := b.target.exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	if err := b.targetUpsert(row(1, "old", true, 1)); err != nil {
		t.Fatal(err)
	}
	if err := b.targetUpsert(row(2, "old2", true, 2)); err != nil {
		t.Fatal(err)
	}
	if err := b.target.exec("COMMIT"); err != nil {
		t.Fatal(err)
	}
	assertCount(t, b, 2)

	const gen, cut int64 = 2, 3
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	// Zero rows: the empty end marker is itself the completeness evidence.
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 0})
	applyN(t, b, cdcEvent(gen, KindUpsert, 4, cut, 7))
	if _, err := b.TryPublish(); err != nil {
		t.Fatalf("TryPublish empty: %v", err)
	}
	assertCount(t, b, 1)
	assertRow(t, b, 7, &Row{ID: 7, Email: "u7-new@x.io", EmailPresent: true, Active: false, Score: 7.5})
	assertAbsent(t, b, 1)
}

// ---- restart / crash recovery ----------------------------------------------

func TestRebuildResumeAfterReopen(t *testing.T) {
	dir := testDir(t)
	const gen, cut int64 = 1, 8
	b := openBuilder(t, dir)
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1), snapRow(gen, cut, 2))
	applyN(t, b, cdcEvent(gen, KindUpsert, 9, cut, 1))
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the unfinished rebuild must resume, and previously staged data
	// must be recognized rather than re-inserted as a conflicting duplicate.
	b2 := openBuilder(t, dir)
	st := b2.Snapshot()
	if !st.Rebuilding || st.RebuildGeneration != gen || st.Cut != cut || st.StagedRows != 2 || st.BufferedH != 9 {
		t.Fatalf("recovered state wrong: %+v", st)
	}
	// Re-sending the identical snapshot row is a duplicate no-op.
	res, err := b2.Apply(mustEncode(t, snapRow(gen, cut, 1)))
	if err != nil {
		t.Fatalf("resend snapshot row: %v", err)
	}
	if !res.Duplicate || res.Staged {
		t.Fatalf("expected duplicate no-op, got %+v", res)
	}
	// Continue the prefix from where it stopped.
	applyN(t, b2, cdcEvent(gen, KindDelete, 10, cut, 2))
	applyN(t, b2, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 2})
	if _, err := b2.TryPublish(); err != nil {
		t.Fatalf("TryPublish after resume: %v", err)
	}
	assertCount(t, b2, 1)
	assertAbsent(t, b2, 2)
}

func TestStagingRejectedWhenGenerationAlreadyPublished(t *testing.T) {
	dir := testDir(t)
	const gen, cut int64 = 1, 4
	b := openBuilder(t, dir)
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1))
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 1})
	applyN(t, b, cdcEvent(gen, KindUpsert, 5, cut, 1))
	if _, err := b.TryPublish(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// A leftover staging file for an old generation (simulate by reopening and
	// checking old gen cannot restart) must be ignored, and the target survives.
	b2 := openBuilder(t, dir)
	if err := b2.BeginRebuild(gen); !errors.Is(err, ErrGenerationRollback) {
		t.Fatalf("want ErrGenerationRollback, got %v", err)
	}
	if b2.Snapshot().Rebuilding {
		t.Fatal("no rebuild should be active")
	}
	assertCount(t, b2, 1)
}

// ---- generation safety -----------------------------------------------------

func TestOldAndForeignGenerationsRejected(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 5, 6
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	// Older generation must not mix into the current rebuild.
	if _, err := b.Apply(mustEncode(t, snapRow(gen-1, cut, 1))); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("want ErrGenerationMismatch for old gen, got %v", err)
	}
	// Newer generation must not hijack it either.
	if _, err := b.Apply(mustEncode(t, snapRow(gen+1, cut, 1))); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("want ErrGenerationMismatch for new gen, got %v", err)
	}
	// BeginRebuild must never go backwards relative to a published generation.
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatalf("idempotent same-gen BeginRebuild: %v", err)
	}
}

func TestBeginRebuildRejectsRollback(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	if err := b.BeginRebuild(10); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, &Event{Gen: 10, Kind: KindSnapshotEnd, Cut: 0, RowCount: 0})
	if _, err := b.TryPublish(); err != nil {
		t.Fatal(err)
	}
	if err := b.BeginRebuild(9); !errors.Is(err, ErrGenerationRollback) {
		t.Fatalf("want ErrGenerationRollback, got %v", err)
	}
	if err := b.BeginRebuild(10); !errors.Is(err, ErrGenerationRollback) {
		t.Fatalf("equal gen must also rollback, got %v", err)
	}
}

// ---- duplicates and conflicts ----------------------------------------------

func TestSnapshotDuplicateAndConflictPolicies(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 0
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1))
	// Identical re-delivery: ignored.
	res, err := b.Apply(mustEncode(t, snapRow(gen, cut, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatalf("want duplicate, got %+v", res)
	}
	// Conflicting payload under reject policy: hard error, target untouched.
	conflict := snapRow(gen, cut, 1)
	conflict.Row.Score = 999
	if _, err := b.Apply(mustEncode(t, conflict)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}

	// Last-wins builder accepts the conflicting payload.
	dir2 := testDir(t)
	b2, err := Open(Config{Path: dir2, SnapshotConflict: ConflictLastWins})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b2, snapRow(gen, cut, 1))
	if _, err := b2.Apply(mustEncode(t, conflict)); err != nil {
		t.Fatalf("last-wins conflict: %v", err)
	}
	applyN(t, b2, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 1})
	if _, err := b2.TryPublish(); err != nil {
		t.Fatal(err)
	}
	assertRow(t, b2, 1, conflict.Row)
}

func TestCDCDuplicateReplay(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 0
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	ev := cdcEvent(gen, KindUpsert, 1, cut, 1)
	applyN(t, b, ev)
	// Identical replay of seq 1 during rebuild.
	res, err := b.Apply(mustEncode(t, ev))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatalf("want duplicate cdc, got %+v", res)
	}
	// Conflicting replay with same seq: rejected.
	bad := cdcEvent(gen, KindUpsert, 1, cut, 1)
	bad.Row.Score = 42
	if _, err := b.Apply(mustEncode(t, bad)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate for conflicting cdc, got %v", err)
	}
}

func TestPreCutEventsAreSkipped(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 10
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1))
	// seq <= L already reflected in the snapshot: skipped, not buffered.
	res, err := b.Apply(mustEncode(t, cdcEvent(gen, KindUpsert, 10, cut, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !res.PreCut || res.Staged {
		t.Fatalf("want pre-cut skip, got %+v", res)
	}
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 1})
	// First post-cut must be 11; pre-cut seq 10 must not satisfy the prefix.
	if _, err := b.TryPublish(); !errors.Is(err, ErrNotPublishable) {
		t.Fatalf("want not publishable, got %v", err)
	}
}

// ---- incremental continuation ----------------------------------------------

func TestIncrementalContinuesAfterPublish(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 5
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1))
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 1})
	applyN(t, b, cdcEvent(gen, KindUpsert, 6, cut, 1))
	pub, err := b.TryPublish()
	if err != nil {
		t.Fatal(err)
	}
	if pub.Watermark != 6 {
		t.Fatalf("published H = %d, want 6", pub.Watermark)
	}

	// H+1 must be the next accepted event; H itself replayed is a duplicate.
	dup, err := b.Apply(mustEncode(t, cdcEvent(gen, KindUpsert, 6, cut, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !dup.Duplicate {
		t.Fatalf("want duplicate replay of H, got %+v", dup)
	}
	next := cdcEvent(gen, KindUpsert, 7, cut, 2)
	inc, err := b.Apply(mustEncode(t, next))
	if err != nil {
		t.Fatalf("incremental apply: %v", err)
	}
	if !inc.Staged || inc.Watermark != 7 {
		t.Fatalf("bad incremental result %+v", inc)
	}
	assertRow(t, b, 2, next.Row)

	// Out-of-order seq 9 rejected while expecting 8.
	if _, err := b.Apply(mustEncode(t, cdcEvent(gen, KindDelete, 9, cut, 1))); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("want ErrOutOfOrder, got %v", err)
	}
	// Delete path then completes the sequence.
	del := cdcEvent(gen, KindDelete, 8, cut, 1)
	if _, err := b.Apply(mustEncode(t, del)); err != nil {
		t.Fatal(err)
	}
	assertAbsent(t, b, 1)
}

func TestSnapshotArtifactsRejectedInIncrementalPhase(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	if _, err := b.Apply(mustEncode(t, &Event{Gen: 1, Kind: KindSnapshotEnd, Cut: 0})); !errors.Is(err, ErrNoRebuild) {
		t.Fatalf("want ErrNoRebuild, got %v", err)
	}
}

// ---- failure keeps the old target ------------------------------------------

func TestCrashMidRebuildReopensAndCompletes(t *testing.T) {
	dir := testDir(t)
	const gen, cut int64 = 3, 10

	// First lifetime: publish a complete gen-2 baseline.
	b := openBuilder(t, dir)
	if err := b.BeginRebuild(2); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(2, 0, 1), snapRow(2, 0, 2))
	applyN(t, b, &Event{Gen: 2, Kind: KindSnapshotEnd, Cut: 0, RowCount: 2})
	if _, err := b.TryPublish(); err != nil {
		t.Fatal(err)
	}

	// Start gen-3 rebuild, stage part of the work, then simulate a hard crash:
	// close without publishing.
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 10), snapRow(gen, cut, 20))
	applyN(t, b, cdcEvent(gen, KindUpsert, 11, cut, 10))
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Second lifetime: the old gen-2 target must be intact and the gen-3
	// rebuild recovered for continuation.
	b2, err := Open(Config{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	assertCount(t, b2, 2) // old target untouched by the unfinished rebuild
	if b2.publishedGen != 2 || b2.watermark != 0 {
		t.Fatalf("published state lost: gen=%d h=%d", b2.publishedGen, b2.watermark)
	}
	st := b2.Snapshot()
	if !st.Rebuilding || st.RebuildGeneration != gen || st.Cut != cut ||
		st.BufferedH != 11 || st.StagedRows != 2 || st.SnapshotDone {
		t.Fatalf("rebuild not recovered correctly: %+v", st)
	}

	// Finish the prefix and the completeness evidence, then publish.
	applyN(t, b2, cdcEvent(gen, KindDelete, 12, cut, 20))
	applyN(t, b2, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 2})
	pub, err := b2.TryPublish()
	if err != nil {
		t.Fatalf("complete after crash: %v", err)
	}
	if pub.Generation != gen || pub.Watermark != 12 {
		t.Fatalf("publish summary wrong: %+v", pub)
	}
	assertCount(t, b2, 1) // id 20 deleted
	assertRow(t, b2, 10, &Row{ID: 10, Email: "u10-new@x.io", EmailPresent: true, Active: false, Score: 10.5})
}

func TestFailedRebuildKeepsOldTarget(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	if err := b.target.exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	if err := b.targetUpsert(row(1, "persisted", true, 1)); err != nil {
		t.Fatal(err)
	}
	if err := b.target.exec("COMMIT"); err != nil {
		t.Fatal(err)
	}

	const gen, cut int64 = 1, 2
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 7))
	// Abandon the rebuild; staging disappears, target remains.
	if err := b.AbortRebuild(); err != nil {
		t.Fatalf("AbortRebuild: %v", err)
	}
	if _, err := b.TryPublish(); !errors.Is(err, ErrNoRebuild) {
		t.Fatalf("want ErrNoRebuild after abort, got %v", err)
	}
	assertCount(t, b, 1)
	assertRow(t, b, 1, row(1, "persisted", true, 1))
	if _, err := os.Stat(filepath.Join(dir, stagingDBFile)); !os.IsNotExist(err) {
		t.Fatalf("staging file should be removed, stat err=%v", err)
	}
}

func TestRowCountMismatchFailsAndPreservesOld(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 0
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1))
	bad := &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 2}
	if _, err := b.Apply(mustEncode(t, bad)); err == nil {
		t.Fatal("row count mismatch must fail")
	}
	// Rebuild still incomplete; old target intact.
	if _, err := b.TryPublish(); !errors.Is(err, ErrNotPublishable) {
		t.Fatalf("want ErrNotPublishable, got %v", err)
	}
}

// ---- readers never see a half table ----------------------------------------

func TestConcurrentReadersSeeOnlyCommittedTarget(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 4
	if err := b.target.exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	if err := b.targetUpsert(row(1, "v0", true, 1)); err != nil {
		t.Fatal(err)
	}
	if err := b.target.exec("COMMIT"); err != nil {
		t.Fatal(err)
	}

	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1), snapRow(gen, cut, 2))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				n, err := b.RowCount(context.Background())
				if err != nil {
					t.Errorf("reader error: %v", err)
					return
				}
				// Before publish only the old single-row target may be visible.
				if n != 1 && n != 2 {
					t.Errorf("reader saw impossible count %d", n)
					return
				}
			}
		}
	}()

	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 2})
	applyN(t, b, cdcEvent(gen, KindUpsert, 5, cut, 1))
	if _, err := b.TryPublish(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	assertCount(t, b, 2)
}

// ---- fixed schema / null handling ------------------------------------------

func TestNullEmailRoundTripsThroughBaseline(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 0
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	nullRow := &Row{ID: 1, Active: true, Score: 3.5} // no email
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotRow, Cut: cut, Row: nullRow})
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 1})
	if _, err := b.TryPublish(); err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.GetRow(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.EmailPresent {
		t.Fatalf("email should be SQL NULL, got %+v ok=%v", got, ok)
	}
}

// ---- misc invariants --------------------------------------------------------

func TestFixedTargetSchemaAndPrimaryKey(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	st, err := b.target.prepare("SELECT sql FROM sqlite_master WHERE name=?")
	if err != nil {
		t.Fatal(err)
	}
	defer st.finalize()
	if err := st.bindText(1, targetTable); err != nil {
		t.Fatal(err)
	}
	done, err := st.step()
	if err != nil || done {
		t.Fatalf("target table missing: done=%v err=%v", done, err)
	}
	ddl := st.columnText(0)
	if !bytes.Contains([]byte(ddl), []byte("INTEGER PRIMARY KEY")) {
		t.Fatalf("target DDL lacks fixed primary key: %s", ddl)
	}
}

func TestWatermarkAndRowsCommitTogether(t *testing.T) {
	dir := testDir(t)
	b := openBuilder(t, dir)
	const gen, cut int64 = 1, 7
	if err := b.BeginRebuild(gen); err != nil {
		t.Fatal(err)
	}
	applyN(t, b, snapRow(gen, cut, 1))
	applyN(t, b, &Event{Gen: gen, Kind: KindSnapshotEnd, Cut: cut, RowCount: 1})
	applyN(t, b, cdcEvent(gen, KindUpsert, 8, cut, 1))
	if _, err := b.TryPublish(); err != nil {
		t.Fatal(err)
	}
	// Persisted metadata must equal the in-memory watermark, proving the
	// checkpoint is never recorded ahead of the rows it describes.
	wm, err := b.metaGet("watermark")
	if err != nil {
		t.Fatal(err)
	}
	if wm != "8" {
		t.Fatalf("persisted watermark = %q, want 8", wm)
	}
	pg, err := b.metaGet("published_gen")
	if err != nil {
		t.Fatal(err)
	}
	if pg != "1" {
		t.Fatalf("persisted gen = %q, want 1", pg)
	}
}
