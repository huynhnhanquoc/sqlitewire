package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

// readFixture is a file with one small table of twenty rows, the shape of a request's read: one Read, one statement
// with a parameter, a few rows scanned.
func readFixture(b *testing.B, statements int) *Database {
	b.Helper()
	database, err := Open(context.Background(), filepath.Join(b.TempDir(), "read.db"), Options{Statements: statements})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = database.Close() })
	if _, err := database.Migrate(context.Background(), "b",
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, email TEXT NOT NULL, status TEXT NOT NULL, amount INTEGER NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	if _, err := database.Write(context.Background(), "b", func(transaction *Transaction) error {
		for i := 1; i <= 20; i++ {
			status := "moi"
			if i%2 == 0 {
				status = "xong"
			}
			if _, err := transaction.Exec(`INSERT INTO orders (id, email, status, amount) VALUES (?, ?, ?, ?)`,
				i, "a@shop.vn", status, i*100); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return database
}

const readStatement = "SELECT `id`, `email`, `status`, `amount` FROM `orders` WHERE (`status` IS ?) ORDER BY `id` DESC LIMIT 5"

func readOnce(database *Database, ctx context.Context) error {
	return database.Read(ctx, func(snapshot *Snapshot) error {
		rows, err := snapshot.Query(readStatement, "moi")
		if err != nil {
			return err
		}
		defer rows.Close()
		var (
			id, amount    int64
			email, status string
		)
		for rows.Next() {
			if err := rows.Scan(&id, &email, &status, &amount); err != nil {
				return err
			}
		}
		return rows.Err()
	})
}

// readModes are the two ways a Database reads: its statements kept prepared (the default), and none kept.
var readModes = []struct {
	name       string
	statements int
}{{"kept", 0}, {"none", -1}}

// BenchmarkRead is one Read with one statement, as a request makes it.
func BenchmarkRead(b *testing.B) {
	for _, mode := range readModes {
		b.Run(mode.name, func(b *testing.B) {
			database := readFixture(b, mode.statements)
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				if err := readOnce(database, ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadParallel is the same Read from as many goroutines as GOMAXPROCS, over the readers' connections.
func BenchmarkReadParallel(b *testing.B) {
	for _, mode := range readModes {
		b.Run(mode.name, func(b *testing.B) {
			database := readFixture(b, mode.statements)
			ctx := context.Background()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := readOnce(database, ctx); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkQuery is the same statement through Query: kept, its result is given again; none, it reads every time.
func BenchmarkQuery(b *testing.B) {
	for _, mode := range []struct {
		name  string
		cache int
	}{{"kept", 0}, {"none", -1}} {
		b.Run(mode.name, func(b *testing.B) {
			database := readFixture(b, 0)
			if mode.cache == -1 {
				database.queries.capacity = 0 // as Options.QueryCache -1
			}
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				result, err := database.Query(ctx, readStatement, "moi")
				if err != nil || len(result.Rows) != 5 {
					b.Fatal(err, len(result.Rows))
				}
			}
		})
	}
}

// BenchmarkQueryParallel is BenchmarkQuery's kept statement from as many goroutines as GOMAXPROCS.
func BenchmarkQueryParallel(b *testing.B) {
	database := readFixture(b, 0)
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := database.Query(ctx, readStatement, "moi"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
