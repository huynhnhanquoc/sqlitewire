package gateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	kitdbsqlite "github.com/huynhnhanquoc/sqlitewire/sqlite"
)

// closingWriters open the writer of a file for a session and close it as the session lets it go, as engine2's shared
// writers do when the session was the last to hold one.
type closingWriters struct{}

func (closingWriters) Open(path string) (Writer, error) {
	return kitdbsqlite.Open(context.Background(), path, kitdbsqlite.Options{})
}

// A session that wrote, once it ends, leaves no -wal beside the file: it closes its own read-only connection before it
// lets its writers go, so a writer it was the last to hold closes the file's last connection, and SQLite removes the
// WAL then (a read-only connection closed last does not). On 12/11/2026 kitwork database stopped by Ctrl+C left a
// -wal beside the file its port wrote: the writers went first.
//
// CONTROL (12/11/2026): with the writers let go before the session's connection, "a -wal of … bytes stays beside the
// file after the session".
func TestASessionThatWroteLeavesNoWAL(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "application", "kitwork.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	made, err := kitdbsqlite.Open(ctx, path, kitdbsqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := made.Migrate(ctx, "boot", `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) STRICT`); err != nil {
		t.Fatal(err)
	}
	if err := made.Close(); err != nil {
		t.Fatal(err)
	}
	access := newAccess()
	access.admin = true
	session, err := Open(ctx, Target{Name: "shop.test/application", Schemas: map[string]string{"public": path}}, access,
		fakeColumns{}, Options{Writers: closingWriters{}, User: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	run(t, session, `INSERT INTO notes (body) VALUES ('một')`)
	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Fatalf("no -wal while the session's writer holds the file: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path + "-wal"); err == nil {
		t.Fatalf("a -wal of %d bytes stays beside the file after the session", info.Size())
	}
}
