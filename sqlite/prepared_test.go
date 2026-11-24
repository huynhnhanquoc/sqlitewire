package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// preparedFixture is a file with orders 1..20, odd ones "moi", even ones "xong", every table's history kept so a write
// commits a revision.
func preparedFixture(t *testing.T, statements int) *Database {
	t.Helper()
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "prepared.db"),
		Options{HistoryOfEveryTable: true, Statements: statements})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.Migrate(context.Background(), "t",
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL, amount INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 20; i++ {
		status := "moi"
		if i%2 == 0 {
			status = "xong"
		}
		mustWrite(t, database, fmt.Sprintf(`INSERT INTO orders (id, status, amount) VALUES (%d, '%s', %d)`, i, status, i*100))
	}
	return database
}

const ordersByStatus = `SELECT id FROM orders WHERE status = ? ORDER BY id`

// idsOf is every id the statement gives, read inside one snapshot.
func idsOf(t *testing.T, database *Database, query string, arguments ...any) (uint64, string) {
	t.Helper()
	var revision uint64
	var ids string
	if err := database.Read(context.Background(), func(snapshot *Snapshot) error {
		revision = snapshot.Revision
		var err error
		ids, err = scanIDs(snapshot, query, arguments...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return revision, ids
}

func scanIDs(snapshot *Snapshot, query string, arguments ...any) (string, error) {
	rows, err := snapshot.Query(query, arguments...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return "", err
		}
		ids = append(ids, fmt.Sprint(id))
	}
	return strings.Join(ids, ","), rows.Err()
}

const oddIDs, evenIDs = "1,3,5,7,9,11,13,15,17,19", "2,4,6,8,10,12,14,16,18,20"

// TestReadKeepsStatementsPrepared: a statement a Read ran is prepared after it, with the revision's; the next Read
// runs both prepared and reads what the first did, the arguments of each Read its own.
func TestReadKeepsStatementsPrepared(t *testing.T) {
	database := preparedFixture(t, 0)
	before := database.statements.served.Load()
	revision, ids := idsOf(t, database, ordersByStatus, "moi")
	if ids != oddIDs {
		t.Fatalf("first read: %s", ids)
	}
	if got := database.statements.len(); got != 2 {
		t.Fatalf("kept %d statements after the first read, want 2 (the revision's and the query)", got)
	}
	if served := database.statements.served.Load() - before; served != 0 {
		t.Fatalf("the first read ran %d statements prepared, want 0: none was kept yet", served)
	}
	again, ids := idsOf(t, database, ordersByStatus, "xong")
	if ids != evenIDs || again != revision {
		t.Fatalf("second read: revision %d ids %s, want %d %s", again, ids, revision, evenIDs)
	}
	if served := database.statements.served.Load() - before; served != 2 {
		t.Fatalf("the second read ran %d statements prepared, want 2", served)
	}
}

// TestReadRunsAKeptStatementTwiceAtOnce: inside one snapshot the same text runs twice with the first one's rows still
// open. The connection has one SQLite statement for it, so the second runs unprepared; run prepared, it would bind the
// statement under the first one's rows.
func TestReadRunsAKeptStatementTwiceAtOnce(t *testing.T) {
	database := preparedFixture(t, 0)
	idsOf(t, database, ordersByStatus, "moi") // kept from here
	err := database.Read(context.Background(), func(snapshot *Snapshot) error {
		first, err := snapshot.Query(ordersByStatus, "moi")
		if err != nil {
			return err
		}
		defer first.Close()
		var ids []string
		if !first.Next() {
			return fmt.Errorf("first: no row (%v)", first.Err())
		}
		var id int64
		if err := first.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, fmt.Sprint(id))
		second, err := scanIDs(snapshot, ordersByStatus, "xong")
		if err != nil {
			return fmt.Errorf("second: %w", err)
		}
		if second != evenIDs {
			return fmt.Errorf("second: %s, want %s", second, evenIDs)
		}
		for first.Next() {
			if err := first.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, fmt.Sprint(id))
		}
		if err := first.Err(); err != nil {
			return err
		}
		if got := strings.Join(ids, ","); got != oddIDs {
			return fmt.Errorf("first: %s, want %s", got, oddIDs)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestStatementsAreBounded: past Options.Statements the least recently used statement is closed; every statement
// still reads, kept or not.
func TestStatementsAreBounded(t *testing.T) {
	database := preparedFixture(t, 2)
	queries := []string{
		`SELECT id FROM orders WHERE status = ? ORDER BY id`,
		`SELECT id FROM orders WHERE status = ? ORDER BY id DESC`,
		`SELECT id FROM orders WHERE status = ? AND amount > 0 ORDER BY id`,
	}
	for round := 0; round < 3; round++ {
		for i, query := range queries {
			_, ids := idsOf(t, database, query, "xong")
			want := evenIDs
			if i == 1 {
				want = "20,18,16,14,12,10,8,6,4,2"
			}
			if ids != want {
				t.Fatalf("round %d, query %d: %s, want %s", round, i, ids, want)
			}
			if kept := database.statements.len(); kept > 2 {
				t.Fatalf("round %d, query %d: %d statements kept, the bound is 2", round, i, kept)
			}
		}
	}
}

// TestStatementsNone: Options.Statements -1 keeps none, and Reads read as before.
func TestStatementsNone(t *testing.T) {
	database := preparedFixture(t, -1)
	for i := 0; i < 3; i++ {
		if _, ids := idsOf(t, database, ordersByStatus, "moi"); ids != oddIDs {
			t.Fatalf("read %d: %s", i, ids)
		}
	}
	if kept, served := database.statements.len(), database.statements.served.Load(); kept != 0 || served != 0 {
		t.Fatalf("kept %d, ran %d prepared, want none", kept, served)
	}
	if _, err := Open(context.Background(), filepath.Join(t.TempDir(), "x.db"), Options{Statements: -2}); err == nil {
		t.Fatal("Statements -2 opened")
	}
}

// TestKeptStatementsFollowTheSchema: a kept statement over * reads the columns a migration added, and one over a
// table dropped and made again reads the new table.
func TestKeptStatementsFollowTheSchema(t *testing.T) {
	database := preparedFixture(t, 0)
	columns := func() string {
		var names []string
		if err := database.Read(context.Background(), func(snapshot *Snapshot) error {
			rows, err := snapshot.Query(`SELECT * FROM orders WHERE id = ?`, 1)
			if err != nil {
				return err
			}
			defer rows.Close()
			names, err = rows.Columns()
			if err != nil {
				return err
			}
			values := make([]any, len(names))
			pointers := make([]any, len(names))
			for i := range values {
				pointers[i] = &values[i]
			}
			for rows.Next() {
				if err := rows.Scan(pointers...); err != nil {
					return err
				}
			}
			return rows.Err()
		}); err != nil {
			t.Fatal(err)
		}
		return strings.Join(names, ",")
	}
	columns() // kept from here
	if got := columns(); got != "id,status,amount" {
		t.Fatalf("columns %s", got)
	}
	if _, err := database.Migrate(context.Background(), "t", `ALTER TABLE orders ADD COLUMN note TEXT`); err != nil {
		t.Fatal(err)
	}
	if got := columns(); got != "id,status,amount,note" {
		t.Fatalf("after ADD COLUMN: columns %s", got)
	}
	if _, err := database.Migrate(context.Background(), "t", `DROP TABLE orders`,
		`CREATE TABLE orders (note TEXT, id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, database, `INSERT INTO orders (note, id) VALUES ('n', 1)`)
	if got := columns(); got != "note,id" {
		t.Fatalf("after the table was made again: columns %s", got)
	}
}

// TestReadStillRefusesWrites: a write is refused each time, and never kept.
func TestReadStillRefusesWrites(t *testing.T) {
	database := preparedFixture(t, 0)
	for i := 0; i < 2; i++ {
		err := database.Read(context.Background(), func(snapshot *Snapshot) error {
			_, err := snapshot.Query(`DELETE FROM orders`)
			return err
		})
		if !errors.Is(err, ErrStatementRefused) {
			t.Fatalf("read %d: %v, want ErrStatementRefused", i, err)
		}
	}
	if database.statements.has(`DELETE FROM orders`) {
		t.Fatal("a write was kept")
	}
	if _, ids := idsOf(t, database, `SELECT id FROM orders ORDER BY id LIMIT 1`); ids != "1" {
		t.Fatalf("orders: %s", ids)
	}
}

// TestKeptRevisionFollowsCommits: the revision's statement, kept, reads each commit's revision.
func TestKeptRevisionFollowsCommits(t *testing.T) {
	database := preparedFixture(t, 0)
	revision, _ := idsOf(t, database, ordersByStatus, "moi")
	for i := 21; i <= 23; i++ {
		committed := mustWrite(t, database, fmt.Sprintf(`INSERT INTO orders (id, status, amount) VALUES (%d, 'moi', 1)`, i))
		read, ids := idsOf(t, database, ordersByStatus, "moi")
		if read != committed || read != revision+uint64(i-20) {
			t.Fatalf("after commit %d: read revision %d, committed %d", i, read, committed)
		}
		if !strings.HasSuffix(ids, fmt.Sprint(i)) {
			t.Fatalf("after commit %d: ids %s", i, ids)
		}
	}
}

// TestKeptStatementsAtOnce: Reads on every connection, under writes and a migration, each reading the rows of its
// revision (run with -race).
func TestKeptStatementsAtOnce(t *testing.T) {
	database := preparedFixture(t, 0)
	ctx := context.Background()
	var wait sync.WaitGroup
	failures := make(chan error, 32)
	for g := 0; g < 16; g++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for i := 0; i < 50; i++ {
				err := database.Read(ctx, func(snapshot *Snapshot) error {
					var count, top int64
					rows, err := snapshot.Query(`SELECT count(*), max(id) FROM orders WHERE status = ?`, "moi")
					if err != nil {
						return err
					}
					defer rows.Close()
					for rows.Next() {
						if err := rows.Scan(&count, &top); err != nil {
							return err
						}
					}
					if err := rows.Err(); err != nil {
						return err
					}
					// odd ids 1..19, then one "moi" row per commit after the fixture
					if count < 10 || top < 19 {
						return fmt.Errorf("revision %d: %d rows up to %d", snapshot.Revision, count, top)
					}
					return nil
				})
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	for i := 21; i <= 40; i++ {
		mustWrite(t, database, fmt.Sprintf(`INSERT INTO orders (id, status, amount) VALUES (%d, 'moi', 1)`, i))
		if i == 30 {
			if _, err := database.Migrate(ctx, "t", `ALTER TABLE orders ADD COLUMN note TEXT`); err != nil {
				t.Fatal(err)
			}
		}
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

// TestClosedKeepsNoStatement: Close closes the kept statements; a Read after it is refused.
func TestClosedKeepsNoStatement(t *testing.T) {
	database := preparedFixture(t, 0)
	idsOf(t, database, ordersByStatus, "moi")
	if database.statements.len() == 0 {
		t.Fatal("nothing kept")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if kept := database.statements.len(); kept != 0 {
		t.Fatalf("%d statements kept after Close", kept)
	}
	if err := database.Read(context.Background(), func(*Snapshot) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Read after Close: %v", err)
	}
}

// TestReadsOnEveryConnectionPrepareAfter: Reads holding every reader connection each run a statement not kept. It is
// prepared after each Read, never during one: preparing takes a free connection, and none is free until a Read ends.
func TestReadsOnEveryConnectionPrepareAfter(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "two.db"), Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wait sync.WaitGroup
	var entered atomic.Int32
	inside := make(chan struct{}) // closed once both Reads hold their connection
	failures := make(chan error, 2)
	for g := 0; g < 2; g++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			failures <- database.Read(ctx, func(snapshot *Snapshot) error {
				if entered.Add(1) == 2 {
					close(inside)
				}
				select {
				case <-inside:
				case <-ctx.Done(): // the other Read failed before holding its connection
					return ctx.Err()
				}
				rows, err := snapshot.Query(fmt.Sprintf(`SELECT %d`, g))
				if err != nil {
					return err
				}
				return rows.Close()
			})
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if ctx.Err() != nil {
		t.Fatal("the Reads waited for the context's end")
	}
	if kept := database.statements.len(); kept != 3 {
		t.Fatalf("kept %d statements, want 3 (the revision's, and each Read's own)", kept)
	}
}
