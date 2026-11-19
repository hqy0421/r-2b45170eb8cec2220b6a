package cdcsqlite

import (
	"strconv"
)

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
func parseInt64(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// ---- target metadata -------------------------------------------------------

func (b *Builder) metaGet(key string) (string, error) {
	st, err := b.target.prepare("SELECT value FROM " + metadataTable + " WHERE key=?")
	if err != nil {
		return "", err
	}
	defer st.finalize()
	if err := st.bindText(1, key); err != nil {
		return "", err
	}
	done, err := st.step()
	if err != nil {
		return "", err
	}
	if done {
		return "", errNoRows
	}
	return st.columnText(0), nil
}

func (b *Builder) metaSet(key, value string) error {
	st, err := b.target.prepare("INSERT INTO " + metadataTable +
		"(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
	if err != nil {
		return err
	}
	defer st.finalize()
	if err := st.bindText(1, key); err != nil {
		return err
	}
	if err := st.bindText(2, value); err != nil {
		return err
	}
	done, err := st.step()
	if err != nil {
		return err
	}
	if !done {
		// INSERT ... ON CONFLICT always terminates with DONE; guard anyway.
		_, err = st.step()
		return err
	}
	return nil
}

// ---- target row DML --------------------------------------------------------

func (b *Builder) targetUpsert(r *Row) error {
	st, err := b.target.prepare(`INSERT INTO ` + targetTable +
		`(id,email,active,score) VALUES(?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET email=excluded.email,
		 	active=excluded.active, score=excluded.score`)
	if err != nil {
		return err
	}
	defer st.finalize()
	if err := st.bindInt64(1, r.ID); err != nil {
		return err
	}
	if r.EmailPresent {
		if err := st.bindText(2, r.Email); err != nil {
			return err
		}
	} else if err := st.bindNull(2); err != nil {
		return err
	}
	if err := st.bindInt64(3, boolToInt(r.Active)); err != nil {
		return err
	}
	if err := st.bindDouble(4, r.Score); err != nil {
		return err
	}
	done, err := st.step()
	if err != nil {
		return err
	}
	if !done {
		_, err = st.step()
		return err
	}
	return nil
}

func (b *Builder) targetDelete(id int64) error {
	st, err := b.target.prepare("DELETE FROM " + targetTable + " WHERE id=?")
	if err != nil {
		return err
	}
	defer st.finalize()
	if err := st.bindInt64(1, id); err != nil {
		return err
	}
	done, err := st.step()
	if err != nil {
		return err
	}
	if !done {
		_, err = st.step()
		return err
	}
	return nil
}

func boolToInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// ---- staging: rebuild state ------------------------------------------------

func (b *Builder) stgSet(key, value string) error {
	st, err := b.staging.prepare(
		"INSERT INTO stg_rebuild_state(key,value) VALUES(?,?) " +
			"ON CONFLICT(key) DO UPDATE SET value=excluded.value")
	if err != nil {
		return err
	}
	defer st.finalize()
	if err := st.bindText(1, key); err != nil {
		return err
	}
	if err := st.bindText(2, value); err != nil {
		return err
	}
	done, err := st.step()
	if err != nil {
		return err
	}
	if !done {
		_, err = st.step()
		return err
	}
	return nil
}

// stgSetDigest stores an empty string for a present-but-empty digest; absence
// is represented by no row so it can be distinguished on recovery.
func (b *Builder) stgSetDigest(digest string) error {
	return b.stgSet("digest", digest)
}

func (db *sqliteDB) scalarInt64(query string) (int64, error) {
	st, err := db.prepare(query)
	if err != nil {
		return 0, err
	}
	defer st.finalize()
	done, err := st.step()
	if err != nil {
		return 0, err
	}
	if done {
		return 0, errNoRows
	}
	if st.columnType(0) == sqliteNull {
		return 0, nil
	}
	return st.columnInt64(0), nil
}

// scalarStringNullable reads a state value that may legitimately be missing.
func (db *sqliteDB) scalarStringNullable(query string) (string, bool, error) {
	st, err := db.prepare(query)
	if err != nil {
		return "", false, err
	}
	defer st.finalize()
	done, err := st.step()
	if err != nil {
		return "", false, err
	}
	if done {
		return "", false, nil
	}
	return st.columnText(0), true, nil
}

// ---- staging: dedup ledger -------------------------------------------------

func (b *Builder) stgSeen(key int64) (bool, error) {
	st, err := b.staging.prepare("SELECT 1 FROM stg_seen WHERE seen_key=?")
	if err != nil {
		return false, err
	}
	defer st.finalize()
	if err := st.bindInt64(1, key); err != nil {
		return false, err
	}
	done, err := st.step()
	if err != nil {
		return false, err
	}
	return !done, nil
}

func (b *Builder) stgInsertSeen(key int64) error {
	st, err := b.staging.prepare("INSERT OR IGNORE INTO stg_seen(seen_key) VALUES(?)")
	if err != nil {
		return err
	}
	defer st.finalize()
	if err := st.bindInt64(1, key); err != nil {
		return err
	}
	done, err := st.step()
	if err != nil {
		return err
	}
	if !done {
		_, err = st.step()
		return err
	}
	return nil
}

// ---- staging: snapshot rows ------------------------------------------------

func (b *Builder) stgUpsertSnapshot(r *Row) error {
	st, err := b.staging.prepare(`INSERT INTO stg_snapshot(id,email,active,score)
		VALUES(?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET email=excluded.email,
			active=excluded.active, score=excluded.score`)
	if err != nil {
		return err
	}
	defer st.finalize()
	if err := st.bindInt64(1, r.ID); err != nil {
		return err
	}
	if r.EmailPresent {
		if err := st.bindText(2, r.Email); err != nil {
			return err
		}
	} else if err := st.bindNull(2); err != nil {
		return err
	}
	if err := st.bindInt64(3, boolToInt(r.Active)); err != nil {
		return err
	}
	if err := st.bindDouble(4, r.Score); err != nil {
		return err
	}
	done, err := st.step()
	if err != nil {
		return err
	}
	if !done {
		_, err = st.step()
		return err
	}
	return nil
}

func (b *Builder) stgSelectRow(id int64) (*Row, bool, error) {
	st, err := b.staging.prepare(
		"SELECT id,email,active,score FROM stg_snapshot WHERE id=?")
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

// ---- staging: buffered CDC -------------------------------------------------

func (b *Builder) stgInsertCDC(ev *Event) error {
	st, err := b.staging.prepare(`INSERT INTO stg_cdc
		(seq,kind,id,email,active,score) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer st.finalize()
	if err := st.bindInt64(1, ev.Seq); err != nil {
		return err
	}
	if err := st.bindText(2, string(ev.Kind)); err != nil {
		return err
	}
	if err := st.bindInt64(3, ev.Row.ID); err != nil {
		return err
	}
	if ev.Kind == KindUpsert && ev.Row.EmailPresent {
		if err := st.bindText(4, ev.Row.Email); err != nil {
			return err
		}
	} else if err := st.bindNull(4); err != nil {
		return err
	}
	if ev.Kind == KindUpsert {
		if err := st.bindInt64(5, boolToInt(ev.Row.Active)); err != nil {
			return err
		}
		if err := st.bindDouble(6, ev.Row.Score); err != nil {
			return err
		}
	} else {
		if err := st.bindNull(5); err != nil {
			return err
		}
		if err := st.bindNull(6); err != nil {
			return err
		}
	}
	done, err := st.step()
	if err != nil {
		return err
	}
	if !done {
		_, err = st.step()
		return err
	}
	return nil
}

func (b *Builder) stgSelectCDC(seq int64) (stgCDCEvent, bool, error) {
	var out stgCDCEvent
	st, err := b.staging.prepare(
		"SELECT seq,kind,id,email,active,score FROM stg_cdc WHERE seq=?")
	if err != nil {
		return out, false, err
	}
	defer st.finalize()
	if err := st.bindInt64(1, seq); err != nil {
		return out, false, err
	}
	done, err := st.step()
	if err != nil {
		return out, false, err
	}
	if done {
		return out, false, nil
	}
	out.Seq = st.columnInt64(0)
	out.Kind = EventKind(st.columnText(1))
	out.ID = st.columnInt64(2)
	if st.columnType(3) != sqliteNull {
		out.Email = st.columnText(3)
		out.EmailPresent = true
	}
	if st.columnType(4) != sqliteNull {
		out.Active = st.columnInt64(4) == 1
	}
	if st.columnType(5) != sqliteNull {
		out.Score = st.columnDouble(5)
	}
	return out, true, nil
}
