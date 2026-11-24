package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// cacheFixture is a file with orders 1..10 (odd "moi", even "xong"), two customers and a view of the "moi" orders.
// No table's history is kept unless options asks: a write to them commits no revision, and the cache must still see it.
func cacheFixture(t *testing.T, options Options) *Database {
	t.Helper()
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "cache.db"), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.Migrate(context.Background(), "t",
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL, amount INTEGER NOT NULL)`,
		`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE VIEW open_orders AS SELECT id, amount FROM orders WHERE status = 'moi'`); err != nil {
		t.Fatal(err)
	}
	statements := []string{`INSERT INTO customers (id, name) VALUES (1, 'An'), (2, 'Binh')`}
	for i := 1; i <= 10; i++ {
		status := "moi"
		if i%2 == 0 {
			status = "xong"
		}
		statements = append(statements, fmt.Sprintf(`INSERT INTO orders (id, status, amount) VALUES (%d, '%s', %d)`, i, status, i*100))
	}
	mustWrite(t, database, statements...)
	return database
}

func mustQuery(t *testing.T, database *Database, query string, arguments ...any) *Result {
	t.Helper()
	result, err := database.Query(context.Background(), query, arguments...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// shown is a result as text: its columns, then each row.
func shown(result *Result) string {
	var text strings.Builder
	text.WriteString(strings.Join(result.Columns, ","))
	for _, row := range result.Rows {
		text.WriteString(fmt.Sprintf(" %v", row))
	}
	return text.String()
}

const moiOrders = `SELECT id, amount FROM orders WHERE status = ? ORDER BY id`

// TestQueryKeepsTheResult: the second Query gives the same Result, read once.
func TestQueryKeepsTheResult(t *testing.T) {
	database := cacheFixture(t, Options{})
	first := mustQuery(t, database, moiOrders, "moi")
	if got := shown(first); got != "id,amount [1 100] [3 300] [5 500] [7 700] [9 900]" {
		t.Fatalf("first: %s", got)
	}
	second := mustQuery(t, database, moiOrders, "moi")
	if second != first {
		t.Fatal("the second Query read again")
	}
	if hits, misses := database.queries.hits.Load(), database.queries.misses.Load(); hits != 1 || misses != 1 {
		t.Fatalf("hits %d misses %d, want 1 1", hits, misses)
	}
}

// TestQueryForgetsOnAWrite: a write to a table the statement reads, one the history does not follow (the revision
// does not move), makes the next Query read again; it gives the rows of the write.
func TestQueryForgetsOnAWrite(t *testing.T) {
	database := cacheFixture(t, Options{})
	first := mustQuery(t, database, moiOrders, "moi")
	revision := database.Revision()
	mustWrite(t, database, `UPDATE orders SET amount = 1 WHERE id = 3`)
	if database.Revision() != revision {
		t.Fatal("the fixture's tables are followed: this test needs a write the history does not see")
	}
	second := mustQuery(t, database, moiOrders, "moi")
	if second == first {
		t.Fatal("the Query after a write gave the result read before it")
	}
	if got := shown(second); got != "id,amount [1 100] [3 1] [5 500] [7 700] [9 900]" {
		t.Fatalf("after the write: %s", got)
	}
}

// TestQueryKeepsOnAWriteElsewhere: a write to a table the statement does not read leaves its result kept.
func TestQueryKeepsOnAWriteElsewhere(t *testing.T) {
	database := cacheFixture(t, Options{})
	first := mustQuery(t, database, moiOrders, "moi")
	mustWrite(t, database, `INSERT INTO customers (id, name) VALUES (3, 'Chi')`)
	if second := mustQuery(t, database, moiOrders, "moi"); second != first {
		t.Fatal("a write to customers made a statement on orders read again")
	}
}

// TestQueryThroughAView: a statement on a view depends on the tables under it.
func TestQueryThroughAView(t *testing.T) {
	database := cacheFixture(t, Options{})
	first := mustQuery(t, database, `SELECT sum(amount) FROM open_orders`)
	if got := shown(first); !strings.HasSuffix(got, "[2500]") {
		t.Fatalf("first: %s", got)
	}
	if again := mustQuery(t, database, `SELECT sum(amount) FROM open_orders`); again != first {
		t.Fatal("not kept")
	}
	mustWrite(t, database, `UPDATE orders SET amount = 0 WHERE id = 1`)
	if got := shown(mustQuery(t, database, `SELECT sum(amount) FROM open_orders`)); !strings.HasSuffix(got, "[2400]") {
		t.Fatalf("after a write under the view: %s", got)
	}
}

// TestQueryForgetsOnASchemaChange: a statement of the schema makes every statement read again, its plan included.
func TestQueryForgetsOnASchemaChange(t *testing.T) {
	database := cacheFixture(t, Options{})
	if got := mustQuery(t, database, `SELECT * FROM orders WHERE id = 1`).Columns; strings.Join(got, ",") != "id,status,amount" {
		t.Fatalf("columns %v", got)
	}
	if _, err := database.Migrate(context.Background(), "t", `ALTER TABLE orders ADD COLUMN note TEXT`); err != nil {
		t.Fatal(err)
	}
	if got := mustQuery(t, database, `SELECT * FROM orders WHERE id = 1`).Columns; strings.Join(got, ",") != "id,status,amount,note" {
		t.Fatalf("after ADD COLUMN: columns %v", got)
	}
	// the view now reads customers: its plan is read again, so a write to customers reaches it
	if _, err := database.Migrate(context.Background(), "t", `DROP VIEW open_orders`,
		`CREATE VIEW open_orders AS SELECT id, id * 10 AS amount FROM customers`); err != nil {
		t.Fatal(err)
	}
	before := shown(mustQuery(t, database, `SELECT sum(amount) FROM open_orders`))
	mustWrite(t, database, `INSERT INTO customers (id, name) VALUES (9, 'Gia')`)
	after := shown(mustQuery(t, database, `SELECT sum(amount) FROM open_orders`))
	if !strings.HasSuffix(before, "[30]") || !strings.HasSuffix(after, "[120]") {
		t.Fatalf("the view over customers: %s, then %s", before, after)
	}
}

// TestQueryNeverKeeps: a statement whose answer can change with no commit is read every time.
func TestQueryNeverKeeps(t *testing.T) {
	database := cacheFixture(t, Options{})
	for _, test := range []struct{ query, why string }{
		{`SELECT random() FROM orders WHERE id = 1`, "it calls random()"},
		{`SELECT datetime('now')`, "it calls datetime()"},
		{`SELECT CURRENT_TIMESTAMP`, "it calls current_timestamp()"},
		{`SELECT date('2026-11-24')`, "it calls date()"},
		{`SELECT name FROM pragma_table_info('orders')`, "it reads a virtual table"},
		{`EXPLAIN SELECT id FROM orders`, "only a SELECT, WITH or VALUES is kept"},
		{`/* a comment */ SELECT 1`, "only a SELECT, WITH or VALUES is kept"},
	} {
		first := mustQuery(t, database, test.query)
		if second := mustQuery(t, database, test.query); second == first {
			t.Errorf("%s: kept", test.query)
		}
		plan := database.queries.plan(test.query)
		if plan == nil || plan.cacheable || plan.why != test.why {
			t.Errorf("%s: plan %+v, want not kept because %q", test.query, plan, test.why)
		}
	}
	if kept := database.queries.len(); kept != 0 {
		t.Fatalf("%d results kept", kept)
	}
	// the deterministic functions of the closed list are kept
	first := mustQuery(t, database, `SELECT count(*), lower(status), coalesce(max(amount), 0) FROM orders GROUP BY status`)
	if mustQuery(t, database, `SELECT count(*), lower(status), coalesce(max(amount), 0) FROM orders GROUP BY status`) != first {
		t.Fatal("count, lower, coalesce, max: not kept")
	}
}

// TestQueryArguments: each set of arguments is its own result; an argument the key cannot hold runs every time.
func TestQueryArguments(t *testing.T) {
	database := cacheFixture(t, Options{})
	moi, xong := mustQuery(t, database, moiOrders, "moi"), mustQuery(t, database, moiOrders, "xong")
	if moi == xong || len(xong.Rows) != 5 || xong.Rows[0][0] != int64(2) {
		t.Fatalf("xong: %s", shown(xong))
	}
	if mustQuery(t, database, moiOrders, "moi") != moi || mustQuery(t, database, moiOrders, "xong") != xong {
		t.Fatal("not kept by argument")
	}
	named := `SELECT id FROM orders WHERE status = :status ORDER BY id`
	first := mustQuery(t, database, named, sql.Named("status", "moi"))
	if second := mustQuery(t, database, named, sql.Named("status", "moi")); second == first || len(second.Rows) != 5 {
		t.Fatalf("a named argument: kept, or %s", shown(second))
	}
}

// TestQueryKeyIsExact: two different statements never share a key.
func TestQueryKeyIsExact(t *testing.T) {
	cases := [][]any{
		{int64(1)}, {1.0}, {"1"}, {true}, {[]byte("1")}, {[]byte{}}, {[]byte(nil)}, {nil},
		{"a", "b"}, {"a\x00", "b"}, {"a", "\x00b"}, {"ab"}, {int64(-1)}, {1.5}, {time.Unix(0, 0).UTC()},
	}
	seen := map[string]int{}
	for i, arguments := range cases {
		key, ok := queryKey("SELECT ?", arguments)
		if !ok {
			t.Fatalf("case %d: not keyed", i)
		}
		if j, twice := seen[key]; twice {
			t.Fatalf("cases %d and %d share the key %q", j, i, key)
		}
		seen[key] = i
	}
	if _, ok := queryKey("SELECT ?", []any{uint64(1 << 63)}); ok {
		t.Fatal("a uint64 past int64 was keyed")
	}
	if _, ok := queryKey("SELECT ?", []any{struct{}{}}); ok {
		t.Fatal("a struct was keyed")
	}
}

// TestQueryCacheIsBounded: past Options.QueryCache the least recently used results go; one bigger than a sixteenth
// of it is never kept.
func TestQueryCacheIsBounded(t *testing.T) {
	database := cacheFixture(t, Options{QueryCache: 16 << 10})
	for i := 0; i < 200; i++ {
		mustQuery(t, database, `SELECT id, amount FROM orders WHERE amount > ? ORDER BY id`, i)
		if size := database.queries.size; size > 16<<10 {
			t.Fatalf("query %d: %d bytes kept, the bound is %d", i, size, 16<<10)
		}
	}
	if kept := database.queries.len(); kept == 0 || kept >= 200 {
		t.Fatalf("%d results kept of 200", kept)
	}
	big := `SELECT ? || id FROM orders WHERE id <= 3`
	padding := strings.Repeat("x", 1000) // three rows of 1 KB: past 16 KB / 16, well under 16 KB
	first := mustQuery(t, database, big, padding)
	if second := mustQuery(t, database, big, padding); second == first {
		t.Fatal("a result past a sixteenth of the cache was kept")
	}
}

// TestQueryCacheNone: Options.QueryCache -1 keeps nothing.
func TestQueryCacheNone(t *testing.T) {
	database := cacheFixture(t, Options{QueryCache: -1})
	first := mustQuery(t, database, moiOrders, "moi")
	if second := mustQuery(t, database, moiOrders, "moi"); second == first || shown(second) != shown(first) {
		t.Fatal("kept with QueryCache -1")
	}
	if _, err := Open(context.Background(), filepath.Join(t.TempDir(), "x.db"), Options{QueryCache: -2}); err == nil {
		t.Fatal("QueryCache -2 opened")
	}
}

// TestQueryAfterMaintain: rows a Maintain changed on the writer's connection are read again.
func TestQueryAfterMaintain(t *testing.T) {
	database := cacheFixture(t, Options{})
	first := mustQuery(t, database, moiOrders, "moi")
	if _, err := database.Maintain(context.Background(), "t", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `UPDATE orders SET amount = 7 WHERE id = 1`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	second := mustQuery(t, database, moiOrders, "moi")
	if second == first || second.Rows[0][1] != int64(7) {
		t.Fatalf("after Maintain: %s", shown(second))
	}
}

// TestQuerySeesAnotherProcess: a write from another connection, which the history does not see, is noticed within
// othersInterval; with no write the results stay kept.
func TestQuerySeesAnotherProcess(t *testing.T) {
	interval := othersInterval
	othersInterval = 20 * time.Millisecond
	defer func() { othersInterval = interval }()
	database := cacheFixture(t, Options{})
	first := mustQuery(t, database, moiOrders, "moi")
	time.Sleep(10 * othersInterval)
	if again := mustQuery(t, database, moiOrders, "moi"); again != first {
		t.Fatal("results were forgotten with no write")
	}
	other, err := sql.Open("sqlite", dataSourceName(database.Path()))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Exec(`UPDATE orders SET amount = 5 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := mustQuery(t, database, moiOrders, "moi"); got.Rows[0][1] == int64(5) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the write of another connection was not noticed in 2 s")
		}
		time.Sleep(othersInterval)
	}
}

// TestQueryReadsItsOwnWrite: a Query begun once a Write returned reads it.
func TestQueryReadsItsOwnWrite(t *testing.T) {
	database := cacheFixture(t, Options{})
	for i := 1; i <= 30; i++ {
		mustQuery(t, database, `SELECT amount FROM orders WHERE id = 2`)
		mustWrite(t, database, fmt.Sprintf(`UPDATE orders SET amount = %d WHERE id = 2`, 1000+i))
		if got := mustQuery(t, database, `SELECT amount FROM orders WHERE id = 2`).Rows[0][0]; got != int64(1000+i) {
			t.Fatalf("write %d: read %v", i, got)
		}
	}
}

// TestQueryAtOnce: Queries from many goroutines under a stream of writes each read a value no older than one they
// read before, and the last write by the end (run with -race).
func TestQueryAtOnce(t *testing.T) {
	database := cacheFixture(t, Options{})
	const writes = 60
	var wait sync.WaitGroup
	stop := make(chan struct{})
	failures := make(chan error, 16)
	for g := 0; g < 12; g++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			last := int64(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				result, err := database.Query(context.Background(), `SELECT amount FROM orders WHERE id = 4`)
				if err != nil {
					failures <- err
					return
				}
				value := result.Rows[0][0].(int64)
				if value < last {
					failures <- fmt.Errorf("read %d after %d", value, last)
					return
				}
				last = value
			}
		}()
	}
	for i := 1; i <= writes; i++ {
		mustWrite(t, database, fmt.Sprintf(`UPDATE orders SET amount = %d WHERE id = 4`, 10000+i))
	}
	close(stop)
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if got := mustQuery(t, database, `SELECT amount FROM orders WHERE id = 4`).Rows[0][0]; got != int64(10000+writes) {
		t.Fatalf("after the writes: %v", got)
	}
}

// TestQueryErrors: a statement that fails keeps nothing; once the table is made it reads.
func TestQueryErrors(t *testing.T) {
	database := cacheFixture(t, Options{})
	if _, err := database.Query(context.Background(), `SELECT id FROM invoices`); err == nil {
		t.Fatal("a missing table read")
	}
	if _, err := database.Query(context.Background(), `DELETE FROM orders`); !errors.Is(err, ErrStatementRefused) {
		t.Fatalf("a write: %v", err)
	}
	if kept := database.queries.len(); kept != 0 {
		t.Fatalf("%d results kept after failures", kept)
	}
	if _, err := database.Migrate(context.Background(), "t", `CREATE TABLE invoices (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, database, `INSERT INTO invoices (id) VALUES (1)`)
	if got := shown(mustQuery(t, database, `SELECT id FROM invoices`)); got != "id [1]" {
		t.Fatalf("invoices: %s", got)
	}
}

// TestQueryClosed: Close forgets every result; a Query after it is refused.
func TestQueryClosed(t *testing.T) {
	database := cacheFixture(t, Options{})
	mustQuery(t, database, moiOrders, "moi")
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if kept := database.queries.len(); kept != 0 {
		t.Fatalf("%d results kept after Close", kept)
	}
	if _, err := database.Query(context.Background(), moiOrders, "moi"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Query after Close: %v", err)
	}
}

// TestQueryKeepsNothingAWriteOvertook: a write committed after a Query's snapshot began and before its result is
// kept makes the result not kept: the next Query reads the write.
func TestQueryKeepsNothingAWriteOvertook(t *testing.T) {
	database := cacheFixture(t, Options{})
	overtake := true
	queryRead = func() {
		if overtake {
			overtake = false
			mustWrite(t, database, `UPDATE orders SET amount = 42 WHERE id = 1`)
		}
	}
	defer func() { queryRead = nil }()
	if got := mustQuery(t, database, moiOrders, "moi").Rows[0][1]; got != int64(100) {
		t.Fatalf("the read before the write: %v", got)
	}
	if kept := database.queries.len(); kept != 0 {
		t.Fatalf("%d results kept: one a write overtook is dropped, not kept to be found stale", kept)
	}
	if got := mustQuery(t, database, moiOrders, "moi").Rows[0][1]; got != int64(42) {
		t.Fatalf("the Query after the write gave %v: a result older than the write was kept", got)
	}
}

// TestQueryThroughAnIndex: a statement answered from an index alone depends on the index's table.
func TestQueryThroughAnIndex(t *testing.T) {
	database := cacheFixture(t, Options{})
	if _, err := database.Migrate(context.Background(), "t", `CREATE INDEX orders_status ON orders (status, amount)`); err != nil {
		t.Fatal(err)
	}
	covered := `SELECT amount FROM orders WHERE status = ? ORDER BY amount`
	var opens []string
	if err := database.Read(context.Background(), func(snapshot *Snapshot) error {
		rows, err := snapshot.tx.QueryContext(snapshot.ctx, "EXPLAIN "+covered, "xong")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var address, p1, p2, p3, p5 int64
			var opcode string
			var p4, comment any
			if err := rows.Scan(&address, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
				return err
			}
			if opcode == "OpenRead" {
				opens = append(opens, fmt.Sprint(p2))
			}
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if len(opens) != 1 {
		t.Fatalf("the statement opens %v, want the index alone", opens)
	}
	first := mustQuery(t, database, covered, "xong")
	if mustQuery(t, database, covered, "xong") != first {
		t.Fatal("not kept")
	}
	mustWrite(t, database, `UPDATE orders SET amount = 1 WHERE id = 2`)
	if got := mustQuery(t, database, covered, "xong").Rows[0][0]; got != int64(1) {
		t.Fatalf("after a write to the index's table: %v", got)
	}
}

// TestQueryAfterMaintainOfBookkeeping: a Maintain that changes only a bookkeeping table commits no Barrier; what it
// changed is read again all the same.
func TestQueryAfterMaintainOfBookkeeping(t *testing.T) {
	database := cacheFixture(t, Options{})
	if _, err := database.Migrate(context.Background(), "t", `CREATE TABLE _kitwork_runs (id INTEGER PRIMARY KEY, state TEXT)`); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, database, `INSERT INTO _kitwork_runs (id, state) VALUES (1, 'waiting')`)
	first := mustQuery(t, database, `SELECT state FROM _kitwork_runs WHERE id = 1`)
	revision := database.Revision()
	if _, err := database.Maintain(context.Background(), "t", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(context.Background(), `UPDATE _kitwork_runs SET state = 'done' WHERE id = 1`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if database.Revision() != revision {
		t.Fatal("the Maintain committed a Barrier: this test needs one that does not")
	}
	second := mustQuery(t, database, `SELECT state FROM _kitwork_runs WHERE id = 1`)
	if second == first || second.Rows[0][0] != "done" {
		t.Fatalf("after Maintain: %s", shown(second))
	}
}

// TestQueryNeverGoesBack: between a COMMIT and the moment it is numbered, a read sees the commit; a Query then must
// not give a result kept from before it.
func TestQueryNeverGoesBack(t *testing.T) {
	database := cacheFixture(t, Options{})
	const statement = `SELECT amount FROM orders WHERE id = 6`
	if got := mustQuery(t, database, statement).Rows[0][0]; got != int64(600) {
		t.Fatalf("before: %v", got)
	}
	var fresh, cached any
	committedHook = func() {
		committedHook = nil
		if err := database.Read(context.Background(), func(snapshot *Snapshot) error {
			return snapshot.tx.QueryRowContext(snapshot.ctx, statement).Scan(&fresh)
		}); err != nil {
			t.Error(err)
			return
		}
		result, err := database.Query(context.Background(), statement)
		if err != nil {
			t.Error(err)
			return
		}
		cached = result.Rows[0][0]
	}
	defer func() { committedHook = nil }()
	mustWrite(t, database, `UPDATE orders SET amount = 601 WHERE id = 6`)
	if fresh != int64(601) {
		t.Fatalf("a read after the COMMIT gave %v: this test needs the commit seen before it is numbered", fresh)
	}
	if cached != int64(601) {
		t.Fatalf("a Query after a read that saw 601 gave %v: a result from before the commit", cached)
	}
}

// TestQueryNeverGoesBackDuringMaintain: a Maintain's work commits as it goes; a read inside it sees its change, and a
// Query then must not give a result kept from before it.
func TestQueryNeverGoesBackDuringMaintain(t *testing.T) {
	database := cacheFixture(t, Options{})
	const statement = `SELECT amount FROM orders WHERE id = 8`
	mustQuery(t, database, statement)
	var fresh, cached any
	if _, err := database.Maintain(context.Background(), "t", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(), `UPDATE orders SET amount = 801 WHERE id = 8`); err != nil {
			return err
		}
		if err := database.Read(context.Background(), func(snapshot *Snapshot) error {
			return snapshot.tx.QueryRowContext(snapshot.ctx, statement).Scan(&fresh)
		}); err != nil {
			return err
		}
		result, err := database.Query(context.Background(), statement)
		if err != nil {
			return err
		}
		cached = result.Rows[0][0]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fresh != int64(801) || cached != int64(801) {
		t.Fatalf("inside Maintain: a read gave %v, then a Query %v", fresh, cached)
	}
}
