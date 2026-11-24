package sqlite

import (
	"container/list"
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
)

// defaultStatements is how many read statements a Database keeps prepared when Options.Statements is zero.
const defaultStatements = 128

// statements are the read statements a Database keeps prepared, most recently used first. Each is prepared on the
// readers' pool, and database/sql keeps it prepared on every reader connection it runs on: a Read runs it through
// Tx.StmtContext, which reuses the connection's statement, so SQLite parses and plans the text once per connection
// instead of once per Read (half of a Read's time, 24/11/2026: BenchmarkRead).
//
// Only a statement classifyStatement read as a read is kept, so one found here needs no classifying again. A
// statement is prepared after the Read that ran it first, never during one: preparing on the pool takes a free
// connection, and a Read holds one, so Reads holding every connection would each wait for another's.
//
// A schema change needs nothing here: SQLite prepares a statement again when the schema it was prepared on changed,
// and the driver reads a statement's columns after its first step, from the statement as prepared again.
type statements struct {
	mu       sync.Mutex
	capacity int
	byText   map[string]*list.Element // of *preparedStatement
	order    list.List                // front: most recently used
	closed   bool

	// served counts the statements a Read ran prepared (tests read it).
	served atomic.Uint64
}

type preparedStatement struct {
	text     string
	prepared *sql.Stmt
}

func newStatements(capacity int) *statements {
	return &statements{capacity: capacity, byText: make(map[string]*list.Element)}
}

// get is the prepared statement of text, nil when none is kept.
func (s *statements) get(text string) *sql.Stmt {
	if s.capacity == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.byText[text]
	if element == nil {
		return nil
	}
	s.order.MoveToFront(element)
	return element.Value.(*preparedStatement).prepared
}

// has reports whether text is kept.
func (s *statements) has(text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byText[text] != nil
}

// put keeps prepared as the statement of text, dropping the least recently used past the capacity. It closes
// prepared instead when text is already kept or the Database closed. database/sql closes a dropped statement once
// the Reads running it are done, and its statement on a connection in use once the connection is free.
func (s *statements) put(text string, prepared *sql.Stmt) {
	s.mu.Lock()
	if s.closed || s.capacity == 0 || s.byText[text] != nil {
		s.mu.Unlock()
		_ = prepared.Close()
		return
	}
	s.byText[text] = s.order.PushFront(&preparedStatement{text: text, prepared: prepared})
	var dropped []*sql.Stmt
	for s.order.Len() > s.capacity {
		oldest := s.order.Back()
		statement := s.order.Remove(oldest).(*preparedStatement)
		delete(s.byText, statement.text)
		dropped = append(dropped, statement.prepared)
	}
	s.mu.Unlock()
	for _, statement := range dropped {
		_ = statement.Close()
	}
}

// len is how many statements are kept.
func (s *statements) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}

// close closes every kept statement; put closes what it is given from then on.
func (s *statements) close() error {
	s.mu.Lock()
	s.closed = true
	var errs []error
	for element := s.order.Front(); element != nil; element = element.Next() {
		errs = append(errs, element.Value.(*preparedStatement).prepared.Close())
	}
	s.byText = make(map[string]*list.Element)
	s.order.Init()
	s.mu.Unlock()
	return errors.Join(errs...)
}

// prepare prepares the statements a Read ran unprepared, once it has given its connection back.
func (database *Database) prepare(ctx context.Context, texts []string) {
	for _, text := range texts {
		if ctx.Err() != nil {
			return
		}
		if database.statements.has(text) {
			continue // another Read prepared it meanwhile
		}
		prepared, err := database.readers.PrepareContext(ctx, text)
		if err != nil {
			continue // it ran, so it prepares; if it no longer does, the next Read runs it unprepared again
		}
		database.statements.put(text, prepared)
	}
}

// query runs text, a read, inside the snapshot: prepared (the kept statement of text, statements.get) when there is
// one and the snapshot has not run it prepared yet, otherwise as it is. A snapshot runs a kept statement prepared
// once: the connection has one SQLite statement for the text, and a second Query of it while the first one's rows
// are open would bind that statement again under them. Unprepared, a text is remembered for prepare after the Read.
func (snapshot *Snapshot) query(prepared *sql.Stmt, text string, arguments ...any) (*sql.Rows, error) {
	if prepared != nil {
		if !snapshot.ranPrepared(text) {
			snapshot.prepared = append(snapshot.prepared, text)
			snapshot.database.statements.served.Add(1)
			return snapshot.tx.StmtContext(snapshot.ctx, prepared).QueryContext(snapshot.ctx, arguments...)
		}
		return snapshot.tx.QueryContext(snapshot.ctx, text, arguments...)
	}
	rows, err := snapshot.tx.QueryContext(snapshot.ctx, text, arguments...)
	if err == nil && snapshot.database.statements.capacity > 0 && !snapshot.missed(text) {
		snapshot.unprepared = append(snapshot.unprepared, text)
	}
	return rows, err
}

func (snapshot *Snapshot) ranPrepared(text string) bool {
	for _, ran := range snapshot.prepared {
		if ran == text {
			return true
		}
	}
	return false
}

func (snapshot *Snapshot) missed(text string) bool {
	for _, ran := range snapshot.unprepared {
		if ran == text {
			return true
		}
	}
	return false
}
