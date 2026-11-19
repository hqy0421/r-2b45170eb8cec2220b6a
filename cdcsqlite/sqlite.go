package cdcsqlite

/*
// The sandbox has no Go SQLite driver available, so talk to the system
// libsqlite3 directly through a tiny cgo binding. Only the symbols used by
// this package are declared; no sqlite3.h development header is required.
//
// Handles are passed across the cgo boundary as integer-sized uintptr values;
// the open/prepare helpers own all result-pointer bookkeeping in C, which keeps
// the Go side free of cgo address-of expressions.
#cgo LDFLAGS: -l:libsqlite3.so.0
#include <stdint.h>
#include <stdlib.h>

#define SQLITE_OK          0
#define SQLITE_ROW         100
#define SQLITE_DONE        101

#define SQLITE_OPEN_READONLY       0x00000001
#define SQLITE_OPEN_READWRITE      0x00000002
#define SQLITE_OPEN_CREATE         0x00000004
#define SQLITE_OPEN_FULLMUTEX      0x00010000

#define SQLITE_NULL    5

#define SQLITE_TRANSIENT ((void (*)(void*))-1)

extern int sqlite3_open_v2(const char*, void**, int, const char*);
extern int sqlite3_close_v2(void*);
extern int sqlite3_exec(void*, const char*, void*, void*, char**);
extern const char *sqlite3_errmsg(void*);
extern int sqlite3_extended_errcode(void*);
extern int sqlite3_busy_timeout(void*, int);
extern int sqlite3_changes(void*);

extern int sqlite3_prepare_v2(void*, const char*, int, void**, const char**);
extern int sqlite3_step(void*);
extern int sqlite3_reset(void*);
extern int sqlite3_finalize(void*);
extern int sqlite3_bind_int64(void*, int, long long);
extern int sqlite3_bind_double(void*, int, double);
extern int sqlite3_bind_null(void*, int);
extern int sqlite3_bind_text(void*, int, const char*, int, void (*)(void*));
extern int sqlite3_bind_blob(void*, int, const void*, int, void (*)(void*));

extern int sqlite3_column_count(void*);
extern int sqlite3_column_type(void*, int);
extern long long sqlite3_column_int64(void*, int);
extern double sqlite3_column_double(void*, int);
extern const unsigned char *sqlite3_column_text(void*, int);
extern const void *sqlite3_column_blob(void*, int);
extern int sqlite3_column_bytes(void*, int);

static long long open_helper(const char* path, int flags) {
	void* db = NULL;
	if (sqlite3_open_v2(path, &db, flags, NULL) != SQLITE_OK) {
		if (db != NULL) sqlite3_close_v2(db);
		return 0;
	}
	sqlite3_busy_timeout(db, 10000);
	return (long long)(intptr_t)db;
}
static long long prepare_helper(void* db, const char* sql) {
	void* stmt = NULL;
	if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return 0;
	return (long long)(intptr_t)stmt;
}
static int exec_helper(void* db, const char* sql) {
	return sqlite3_exec(db, sql, NULL, NULL, NULL);
}
static int bind_text_helper(uintptr_t h, int idx, const char* s, int n) {
	return sqlite3_bind_text((void*)h, idx, s, n, SQLITE_TRANSIENT);
}
static int bind_blob_helper(uintptr_t h, int idx, const void* p, int n) {
	return sqlite3_bind_blob((void*)h, idx, p, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	sqliteOK   = C.SQLITE_OK
	sqliteRow  = C.SQLITE_ROW
	sqliteDone = C.SQLITE_DONE

	sqliteOpenReadOnly  = C.SQLITE_OPEN_READONLY
	sqliteOpenReadWrite = C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_FULLMUTEX

	sqliteNull = C.SQLITE_NULL
)

// sqliteDB is a minimal, single-connection wrapper around the system SQLite.
// The handle is an opaque integer across the Go boundary.
type sqliteDB struct {
	handle uintptr
}

func openSQLite(path string, flags int) (*sqliteDB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	h := uintptr(C.open_helper(cpath, C.int(flags)))
	if h == 0 {
		// The helper closed the half-opened handle on failure; surface a stable
		// message because there is no live db handle to read errmsg from.
		return nil, fmt.Errorf("sqlite open %q: failed (check path and permissions)", path)
	}
	return &sqliteDB{handle: h}, nil
}

func (db *sqliteDB) ptr() unsafe.Pointer { return unsafe.Pointer(db.handle) }

func (db *sqliteDB) close() error {
	if db.handle == 0 {
		return nil
	}
	rc := C.sqlite3_close_v2(db.ptr())
	db.handle = 0
	if rc != sqliteOK {
		return fmt.Errorf("sqlite close: %d", int(rc))
	}
	return nil
}

func (db *sqliteDB) err(prefix string) error {
	code := int(C.sqlite3_extended_errcode(db.ptr()))
	msg := C.GoString(C.sqlite3_errmsg(db.ptr()))
	return fmt.Errorf("%s: %s (code %d)", prefix, msg, code)
}

// exec runs one or more SQL statements without bound parameters.
func (db *sqliteDB) exec(sql string) error {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	if rc := C.exec_helper(db.ptr(), csql); rc != sqliteOK {
		return db.err("sqlite exec")
	}
	return nil
}

// changes returns the number of rows modified by the most recent statement.
func (db *sqliteDB) changes() int {
	return int(C.sqlite3_changes(db.ptr()))
}

func (db *sqliteDB) begin() error { return db.exec("BEGIN IMMEDIATE") }
func (db *sqliteDB) commit() error {
	if err := db.exec("COMMIT"); err != nil {
		_ = db.exec("ROLLBACK")
		return err
	}
	return nil
}
func (db *sqliteDB) rollback() { _ = db.exec("ROLLBACK") }

// stmt is a prepared statement. A statement is not safe for concurrent use;
// the Builder serializes access through its own mutex.
type stmt struct {
	db     *sqliteDB
	handle uintptr
}

func (db *sqliteDB) prepare(sql string) (*stmt, error) {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	h := uintptr(C.prepare_helper(db.ptr(), csql))
	if h == 0 {
		return nil, db.err("sqlite prepare")
	}
	return &stmt{db: db, handle: h}, nil
}

func (s *stmt) ptr() unsafe.Pointer { return unsafe.Pointer(s.handle) }

func (s *stmt) finalize() {
	if s.handle != 0 {
		C.sqlite3_finalize(s.ptr())
		s.handle = 0
	}
}

func (s *stmt) reset() {
	C.sqlite3_reset(s.ptr())
}

func (s *stmt) bindInt64(idx int, v int64) error {
	if rc := C.sqlite3_bind_int64(s.ptr(), C.int(idx), C.longlong(v)); rc != sqliteOK {
		return s.db.err("sqlite bind int64")
	}
	return nil
}

func (s *stmt) bindDouble(idx int, v float64) error {
	if rc := C.sqlite3_bind_double(s.ptr(), C.int(idx), C.double(v)); rc != sqliteOK {
		return s.db.err("sqlite bind double")
	}
	return nil
}

func (s *stmt) bindNull(idx int) error {
	if rc := C.sqlite3_bind_null(s.ptr(), C.int(idx)); rc != sqliteOK {
		return s.db.err("sqlite bind null")
	}
	return nil
}

// bindText binds a Go string; SQLite makes its own copy (SQLITE_TRANSIENT).
func (s *stmt) bindText(idx int, v string) error {
	cv := C.CString(v)
	rc := C.bind_text_helper(C.uintptr_t(s.handle), C.int(idx), cv, C.int(len(v)))
	C.free(unsafe.Pointer(cv))
	if rc != sqliteOK {
		return s.db.err("sqlite bind text")
	}
	return nil
}

// bindBlob binds Go bytes; SQLite makes its own copy (SQLITE_TRANSIENT).
func (s *stmt) bindBlob(idx int, v []byte) error {
	var ptr unsafe.Pointer
	if len(v) > 0 {
		ptr = unsafe.Pointer(&v[0])
	}
	if rc := C.bind_blob_helper(C.uintptr_t(s.handle), C.int(idx), ptr, C.int(len(v))); rc != sqliteOK {
		return s.db.err("sqlite bind blob")
	}
	return nil
}

// step advances the statement. done reports SQLITE_DONE; otherwise the row is
// live and column* accessors may be used.
func (s *stmt) step() (done bool, err error) {
	rc := C.sqlite3_step(s.ptr())
	switch rc {
	case sqliteDone:
		return true, nil
	case sqliteRow:
		return false, nil
	default:
		return false, s.db.err("sqlite step")
	}
}

func (s *stmt) columnCount() int {
	return int(C.sqlite3_column_count(s.ptr()))
}

func (s *stmt) columnType(idx int) int {
	return int(C.sqlite3_column_type(s.ptr(), C.int(idx)))
}

func (s *stmt) columnInt64(idx int) int64 {
	return int64(C.sqlite3_column_int64(s.ptr(), C.int(idx)))
}

func (s *stmt) columnDouble(idx int) float64 {
	return float64(C.sqlite3_column_double(s.ptr(), C.int(idx)))
}

func (s *stmt) columnText(idx int) string {
	cstr := C.sqlite3_column_text(s.ptr(), C.int(idx))
	if cstr == nil {
		return ""
	}
	n := C.sqlite3_column_bytes(s.ptr(), C.int(idx))
	return C.GoStringN((*C.char)(unsafe.Pointer(cstr)), n)
}

// columnBlob returns a copy of the BLOB so its memory never escapes the
// SQLite row buffer.
func (s *stmt) columnBlob(idx int) []byte {
	n := int(C.sqlite3_column_bytes(s.ptr(), C.int(idx)))
	if n == 0 {
		return nil
	}
	p := C.sqlite3_column_blob(s.ptr(), C.int(idx))
	if p == nil {
		return nil
	}
	out := make([]byte, n)
	copy(out, C.GoBytes(p, C.int(n)))
	return out
}

// errNoRows is returned by query helpers that expect a single row.
var errNoRows = errors.New("cdcsqlite: no rows")
