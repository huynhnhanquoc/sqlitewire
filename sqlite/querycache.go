package sqlite

import (
	"container/list"
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The query cache keeps the result of a read statement, by its text and arguments, and gives it again while no commit
// changed a table the statement reads (KITWORK_APPLICATION.md mục 12, 24/11/2026: every request runs the host's
// guards, each reading KitDB). It is exact for every write through the Database, the PostgreSQL gateway's included,
// because one writer makes them all and numbers each commit before answering it: a result is given only while every
// table it read is unchanged since the moment just before it was read.
//
//   - Which tables a statement reads, SQLite says: EXPLAIN of it, in the snapshot that reads it, names the root page of
//     every table and index it opens, the tables under a view included (Snapshot.plan). A statement whose answer could
//     change with no commit is not kept: one calling a function outside a closed list of deterministic ones (random(),
//     datetime('now'), a function the program registered), one reading a virtual, temporary or attached table, one
//     that is not a SELECT, WITH or VALUES.
//   - A commit is numbered after its COMMIT returned and before its requests are answered (writer.publish), with the
//     tables the pre-update hook saw a row of change in, followed by the history or not; a schema statement or a
//     Maintain counts as changing every table. Before the COMMIT the same tables are marked changing, and no result of
//     them is given or kept until the number comes: the COMMIT shows the change to new snapshots first, and a reader
//     that read it must never be given a result kept from before it.
//   - A result is read under the number of the last commit taken just before its snapshot began, and kept only if no
//     table it read has a later number by the time it is stored: the snapshot holds every commit up to that number, so
//     what is kept is never older than its number says.
//   - Another process writing the file is outside what Database promises (the history does not see it either); the
//     writer notices it by PRAGMA data_version within a second and the cache forgets every result (noticeOthers).

// defaultQueryCache is how many bytes of results a Database keeps when Options.QueryCache is zero.
const defaultQueryCache = 8 << 20

// planLimit bounds the plans kept, one per statement text; past it they are all dropped and read again.
const planLimit = 4096

// Result is the rows of one read statement. A Result the cache gives is shared by every caller it gives it to: read
// it, never change it.
type Result struct {
	Columns []string
	Rows    [][]any
}

// tableVersions numbers the writer's commits and keeps, for each table (lower case), the number of the last commit
// that changed it. Only the writer goroutine moves them (writer.publish).
type tableVersions struct {
	sequence   atomic.Uint64
	everything atomic.Uint64 // the number of the last commit that may have changed any table

	mu     sync.RWMutex
	tables map[string]*atomic.Uint64
}

func newTableVersions() *tableVersions {
	return &tableVersions{tables: map[string]*atomic.Uint64{}}
}

// table is the number of the last commit that changed name, made on first use.
func (v *tableVersions) table(name string) *atomic.Uint64 {
	name = strings.ToLower(name)
	v.mu.RLock()
	version := v.tables[name]
	v.mu.RUnlock()
	if version != nil {
		return version
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if version = v.tables[name]; version == nil {
		version = new(atomic.Uint64)
		v.tables[name] = version
	}
	return version
}

// changingVersion is the number of a table being changed: later than any result's, so none of it is given or kept.
const changingVersion = math.MaxUint64

// changing marks touched, or every table, as being changed, until committed numbers them. The writer marks them
// before its COMMIT, because the COMMIT shows the change to new snapshots before committed can run: in between, a read
// could show the change, and a result kept before it, given after it, would not (a reader would go back in time).
func (v *tableVersions) changing(touched map[string]struct{}, everything bool) {
	for name := range touched {
		v.table(name).Store(changingVersion)
	}
	if everything {
		v.everything.Store(changingVersion)
	}
}

// committed numbers one commit, which changed touched, or every table. The tables take the number before the
// sequence does, so a reader that sees the sequence at n sees every table changed by a commit up to n.
func (v *tableVersions) committed(touched map[string]struct{}, everything bool) {
	if len(touched) == 0 && !everything {
		return
	}
	next := v.sequence.Load() + 1
	for name := range touched {
		v.table(name).Store(next)
	}
	if everything {
		v.everything.Store(next)
	}
	v.sequence.Store(next)
}

// unchangedSince reports that no commit numbered after since changed any of tables.
func (v *tableVersions) unchangedSince(since uint64, tables []*atomic.Uint64) bool {
	if v.everything.Load() > since {
		return false
	}
	for _, table := range tables {
		if table.Load() > since {
			return false
		}
	}
	return true
}

// queryPlan is what the cache knows of one statement text: whether its result can be kept, and the tables it reads.
type queryPlan struct {
	since     uint64 // read in a snapshot that began after commit since; good while no later commit changed the schema
	cacheable bool
	tables    []*atomic.Uint64
	why       string // why it is not kept
}

type cachedResult struct {
	key    string
	since  uint64
	tables []*atomic.Uint64
	result *Result
	size   int64
}

type queryCache struct {
	capacity int64
	versions *tableVersions

	mu      sync.Mutex
	size    int64
	results map[string]*list.Element // of *cachedResult
	order   list.List                // front: most recently used
	plans   map[string]*queryPlan
	closed  bool

	// hits and misses count the Query calls a kept result answered and the ones that read (tests read them).
	hits, misses atomic.Uint64
}

func newQueryCache(capacity int64, versions *tableVersions) *queryCache {
	return &queryCache{capacity: capacity, versions: versions, results: map[string]*list.Element{},
		plans: map[string]*queryPlan{}}
}

// get is the kept result of key, nil when there is none or a commit changed a table it read.
func (c *queryCache) get(key string) *Result {
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.results[key]
	if element == nil {
		return nil
	}
	kept := element.Value.(*cachedResult)
	if !c.versions.unchangedSince(kept.since, kept.tables) {
		c.removeLocked(element)
		return nil
	}
	c.order.MoveToFront(element)
	return kept.result
}

// put keeps result, read under commit since from tables, unless a commit changed one of them meanwhile or it would
// take more than a sixteenth of the cache.
func (c *queryCache) put(key string, since uint64, tables []*atomic.Uint64, result *Result) {
	size := resultSize(key, result)
	if size > c.capacity/16 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.versions.unchangedSince(since, tables) {
		return
	}
	if element := c.results[key]; element != nil {
		c.removeLocked(element)
	}
	c.results[key] = c.order.PushFront(&cachedResult{key: key, since: since, tables: tables, result: result, size: size})
	c.size += size
	for c.size > c.capacity {
		c.removeLocked(c.order.Back())
	}
}

func (c *queryCache) removeLocked(element *list.Element) {
	kept := c.order.Remove(element).(*cachedResult)
	delete(c.results, kept.key)
	c.size -= kept.size
}

// plan is the plan of text, nil when there is none or the schema changed since it was read.
func (c *queryCache) plan(text string) *queryPlan {
	c.mu.Lock()
	defer c.mu.Unlock()
	plan := c.plans[text]
	if plan != nil && c.versions.everything.Load() > plan.since {
		delete(c.plans, text)
		return nil
	}
	return plan
}

func (c *queryCache) putPlan(text string, plan *queryPlan) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.versions.everything.Load() > plan.since {
		return
	}
	if len(c.plans) >= planLimit {
		clear(c.plans)
	}
	c.plans[text] = plan
}

// len is how many results are kept.
func (c *queryCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *queryCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	clear(c.results)
	clear(c.plans)
	c.order.Init()
	c.size = 0
}

// Query runs query, one read statement, on the latest commit and gives its rows. The result is kept (up to
// Options.QueryCache bytes) and given again, the same Result, while no commit changes a table the statement reads;
// the doc of the query cache (querycache.go) says which statements are never kept. The arguments of a kept statement
// are nil, integers, floats, booleans, strings, byte slices or times; with any other, the statement runs every time.
func (database *Database) Query(ctx context.Context, query string, arguments ...any) (*Result, error) {
	if err := database.ensureOpen(); err != nil {
		return nil, err
	}
	cache := database.queries
	key, keyed := "", false
	if cache.capacity > 0 {
		key, keyed = queryKey(query, arguments)
	}
	if keyed {
		if result := cache.get(key); result != nil {
			cache.hits.Add(1)
			return result, nil
		}
		cache.misses.Add(1)
	}
	// before the snapshot begins: it holds every commit up to since, perhaps some after
	since := cache.versions.sequence.Load()
	var plan *queryPlan
	if keyed {
		plan = cache.plan(query)
	}
	derive := keyed && plan == nil
	var result *Result
	err := database.Read(ctx, func(snapshot *Snapshot) error {
		if derive {
			plan = snapshot.plan(query, arguments, since, cache.versions)
		}
		var err error
		result, err = snapshot.result(query, arguments)
		return err
	})
	if err != nil {
		return nil, err
	}
	if queryRead != nil {
		queryRead()
	}
	if plan != nil {
		if derive {
			cache.putPlan(query, plan)
		}
		if plan.cacheable {
			cache.put(key, since, plan.tables, result)
		}
	}
	return result, nil
}

// queryRead, set by tests only, runs after Query read a result and before it is kept: a write it makes overtook the
// read.
var queryRead func()

// result reads every row of query into a Result.
func (snapshot *Snapshot) result(query string, arguments []any) (*Result, error) {
	rows, err := snapshot.Query(query, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := &Result{Columns: columns, Rows: [][]any{}}
	pointers := make([]any, len(columns))
	for rows.Next() {
		row := make([]any, len(columns))
		for i := range row {
			pointers[i] = &row[i]
		}
		if err := rows.Scan(pointers...); err != nil { // database/sql copies a []byte scanned into an *any
			return nil, err
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, rows.Close()
}

// openRegister is OPFLAG_P2ISREG of SQLite: P2 of an Open is a register holding the root page, not the page.
const openRegister = 0x10

// plan reads, in the snapshot, what the cache needs to know of query: the tables EXPLAIN opens, and whether anything
// in it could change its answer with no commit. It is nil when EXPLAIN fails (the statement runs and fails, or runs
// unkept).
func (snapshot *Snapshot) plan(query string, arguments []any, since uint64, versions *tableVersions) *queryPlan {
	plan := &queryPlan{since: since}
	switch leadingWord(query) {
	case "select", "with", "values":
	default:
		plan.why = "only a SELECT, WITH or VALUES is kept"
		return plan
	}
	roots, err := snapshot.rootPages()
	if err != nil {
		return nil
	}
	rows, err := snapshot.tx.QueryContext(snapshot.ctx, "EXPLAIN "+query, arguments...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	names := map[string]bool{}
	for rows.Next() {
		var (
			address, p1, p2, p3, p5 int64
			opcode                  string
			p4, comment             any
		)
		if err := rows.Scan(&address, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			return nil
		}
		if plan.why != "" {
			continue
		}
		switch opcode {
		case "OpenRead", "ReopenIdx":
			name, known := roots[p2]
			switch {
			case p3 != 0:
				plan.why = "it reads a temporary or attached table"
			case p5&openRegister != 0 || !known:
				plan.why = "it opens a table EXPLAIN does not name"
			default:
				names[name] = true
			}
		case "OpenWrite":
			plan.why = "it writes"
		case "VOpen":
			plan.why = "it reads a virtual table"
		case "Function", "PureFunc", "AggStep", "AggStep1", "AggValue", "AggInverse", "AggFinal":
			text, _ := p4.(string)
			if name := functionName(text); !deterministic[name] {
				plan.why = "it calls " + name + "()"
			}
		}
	}
	if rows.Err() != nil {
		return nil
	}
	if plan.why == "" {
		plan.cacheable = true
		for name := range names {
			plan.tables = append(plan.tables, versions.table(name))
		}
	}
	return plan
}

// rootPages names, by root page, the table each table and index of the file belongs to.
func (snapshot *Snapshot) rootPages() (map[int64]string, error) {
	rows, err := snapshot.tx.QueryContext(snapshot.ctx, `SELECT tbl_name, rootpage FROM sqlite_schema WHERE rootpage > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	roots := map[int64]string{1: "sqlite_schema"}
	for rows.Next() {
		var table string
		var root int64
		if err := rows.Scan(&table, &root); err != nil {
			return nil, err
		}
		roots[root] = table
	}
	return roots, rows.Err()
}

// leadingWord is the first word of query in lower case, empty when a comment or anything else comes first.
func leadingWord(query string) string {
	query = strings.TrimLeft(query, " \t\r\n(")
	end := 0
	for end < len(query) && (query[end] >= 'a' && query[end] <= 'z' || query[end] >= 'A' && query[end] <= 'Z') {
		end++
	}
	return strings.ToLower(query[:end])
}

// functionName is the name in P4 of a function instruction of EXPLAIN, "count(0)" or "->>(2)".
func functionName(p4 string) string {
	if open := strings.LastIndexByte(p4, '('); open > 0 {
		p4 = p4[:open]
	}
	return strings.ToLower(p4)
}

// deterministic are the functions of SQLite whose answer depends on their arguments and the rows alone. The date and
// time functions are left out even with a fixed date: 'now' is one of their arguments.
var deterministic = map[string]bool{}

func init() {
	for _, name := range strings.Fields(`
		count sum total avg min max group_concat string_agg
		row_number rank dense_rank percent_rank cume_dist ntile lag lead first_value last_value nth_value
		abs char coalesce concat concat_ws format printf glob hex ifnull iif if instr length octet_length like
		likelihood likely unlikely lower upper ltrim rtrim trim nullif quote replace round sign substr substring
		typeof unhex unicode zeroblob
		acos acosh asin asinh atan atan2 atanh ceil ceiling cos cosh degrees exp floor ln log log2 log10 mod pi pow
		power radians sin sinh sqrt tan tanh trunc
		json json_array json_array_length json_error_position json_extract json_insert json_object json_patch
		json_pretty json_remove json_replace json_set json_type json_valid json_quote json_group_array
		json_group_object jsonb jsonb_array jsonb_extract jsonb_insert jsonb_object jsonb_patch jsonb_remove
		jsonb_replace jsonb_set jsonb_group_array jsonb_group_object -> ->>`) {
		deterministic[name] = true
	}
}

// queryKey is the cache's key of a statement and its arguments; false when an argument has a type it does not key.
func queryKey(text string, arguments []any) (string, bool) {
	var key strings.Builder
	key.Grow(len(text) + 1 + 12*len(arguments))
	key.WriteString(text)
	for _, argument := range arguments {
		key.WriteByte(0)
		switch value := argument.(type) {
		case nil:
			key.WriteByte('n')
		case int64:
			writeInteger(&key, value)
		case int:
			writeInteger(&key, int64(value))
		case int32:
			writeInteger(&key, int64(value))
		case int16:
			writeInteger(&key, int64(value))
		case int8:
			writeInteger(&key, int64(value))
		case uint8:
			writeInteger(&key, int64(value))
		case uint16:
			writeInteger(&key, int64(value))
		case uint32:
			writeInteger(&key, int64(value))
		case uint:
			if uint64(value) > math.MaxInt64 {
				return "", false
			}
			writeInteger(&key, int64(value))
		case uint64:
			if value > math.MaxInt64 {
				return "", false
			}
			writeInteger(&key, int64(value))
		case float64:
			key.WriteByte('f')
			key.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
		case float32:
			key.WriteByte('f')
			key.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 64))
		case bool:
			if value {
				key.WriteString("b1")
			} else {
				key.WriteString("b0")
			}
		case string:
			key.WriteByte('s')
			key.WriteString(strconv.Itoa(len(value)))
			key.WriteByte(':')
			key.WriteString(value)
		case []byte:
			if value == nil {
				key.WriteByte('X')
				break
			}
			key.WriteByte('x')
			key.WriteString(strconv.Itoa(len(value)))
			key.WriteByte(':')
			key.Write(value)
		case time.Time:
			key.WriteByte('t')
			key.WriteString(value.Format(time.RFC3339Nano))
		default:
			return "", false // sql.NamedArg, a driver.Valuer, …
		}
	}
	return key.String(), true
}

func writeInteger(key *strings.Builder, value int64) {
	key.WriteByte('i')
	key.WriteString(strconv.FormatInt(value, 10))
}

// resultSize is about how many bytes a kept result holds.
func resultSize(key string, result *Result) int64 {
	size := int64(len(key)) + 128
	for _, column := range result.Columns {
		size += int64(len(column)) + 16
	}
	for _, row := range result.Rows {
		size += 24 + 16*int64(len(row))
		for _, value := range row {
			switch value := value.(type) {
			case string:
				size += int64(len(value))
			case []byte:
				size += int64(len(value))
			case time.Time:
				size += 24
			}
		}
	}
	return size
}
