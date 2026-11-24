package sqlite

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// backupWriterFile names the file TestBackupFileWriter opens; set, the test binary run with
// -test.run=^TestBackupFileWriter$ is the other process of TestBackupFileWhileAnotherProcessWrites.
const backupWriterFile = "KITDB_SQLITE_BACKUP_WRITER"

// TestBackupFileWriter is the server of TestBackupFileWhileAnotherProcessWrites, in a process of its own: it opens the
// file, which takes its writer, says "ready <revision>", writes a row a commit, says "writing" after fifty, and goes on
// until its standard input closes; then it says "wrote <rows>" and closes the file.
func TestBackupFileWriter(t *testing.T) {
	path := os.Getenv(backupWriterFile)
	if path == "" {
		t.Skip("runs as the writer of TestBackupFileWhileAnotherProcessWrites")
	}
	ctx := context.Background()
	database, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		stop.Store(true)
	}()
	os.Stdout.WriteString("ready " + strconv.FormatUint(database.Revision(), 10) + "\n")
	rows := 0
	deadline := time.Now().Add(2 * time.Minute) // the parent gone without closing the pipe
	for !stop.Load() && time.Now().Before(deadline) {
		if _, err := database.Write(ctx, "writer", func(transaction *Transaction) error {
			_, err := transaction.Exec(`INSERT INTO notes (body) VALUES (?)`, strings.Repeat("w", 2000))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if rows++; rows == 50 {
			os.Stdout.WriteString("writing\n")
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout.WriteString("wrote " + strconv.Itoa(rows) + "\n")
}

// TestBackupFileWhileAnotherProcessWrites: BackupFile copies a KitDB file whose writer another process holds, as a
// command beside a running server does (KITWORK_COMMANDS.md mục 5 và 6), while that process goes on writing; the copy
// opens as a database at the revision BackupFile returned, with every row a commit up to it wrote and none after. With
// the writer gone it copies the file as it is and leaves nothing beside it. A file that is not there, or that KitDB did
// not write, is refused, and leaves nothing.
//
// CONTROL (13/11/2026): with BackupFile taking the writer, as Database.Backup needed it taken, "BackupFile while
// another process holds the writer: kitdb: lock …: kitdb: database already has a writer"; with the connection opened
// mode=ro as Backup's was, "BackupFile with no writer left [kitwork.db kitwork.db-shm kitwork.db-wal …] beside the
// file, which had [kitwork.db …]"; with the file KitDB did not write not refused, "BackupFile of a file KitDB did not
// write: SQL logic error: no such table: kitwork_history (1)"; with the file not looked for first, "BackupFile of a
// file that is not there: unable to open database file (14)".
func TestBackupFileWhileAnotherProcessWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a writer in another process")
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kitwork.db")
	database, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Migrate(ctx, "boot", `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetHistory(ctx, "t", "notes", true); err != nil {
		t.Fatal(err)
	}
	// a file of some megabytes, so the copy takes long enough for the writer to commit while it is taken
	const before = 3000
	for i := 0; i < before; i += 100 {
		if _, err := database.Write(ctx, "t", func(transaction *Transaction) error {
			for j := 0; j < 100; j++ {
				if _, err := transaction.Exec(`INSERT INTO notes (body) VALUES (?)`, strings.Repeat("b", 2000)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	writer := exec.Command(os.Args[0], "-test.run=^TestBackupFileWriter$", "-test.count=1")
	writer.Env = append(os.Environ(), backupWriterFile+"="+path)
	stdin, err := writer.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := writer.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	writer.Stderr = &stderr
	if err := writer.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() string {
		stopped = true
		_ = stdin.Close()
		rest, _ := io.ReadAll(stdout)
		if err := writer.Wait(); err != nil {
			t.Fatalf("the writer: %v\n%s%s", err, rest, stderr.String())
		}
		return string(rest)
	}
	t.Cleanup(func() {
		if !stopped {
			_ = stdin.Close()
			_ = writer.Process.Kill()
			_ = writer.Wait()
		}
	})
	lines := bufio.NewScanner(stdout)
	expect := func(word string) string {
		for lines.Scan() {
			if fields := strings.Fields(lines.Text()); len(fields) > 0 && fields[0] == word {
				return strings.TrimSpace(strings.TrimPrefix(lines.Text(), word))
			}
		}
		_ = writer.Process.Kill()
		_ = writer.Wait()
		stopped = true
		t.Fatalf("the writer did not say %q: %v\n%s", word, lines.Err(), stderr.String())
		return ""
	}
	first, err := strconv.ParseUint(expect("ready"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	expect("writing")
	if _, err := Open(ctx, path, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("Open while another process holds the writer: %v, want ErrLocked", err)
	}

	target := filepath.Join(t.TempDir(), "elsewhere", "kitwork.db")
	revision, err := BackupFile(ctx, path, target)
	if err != nil {
		t.Fatalf("BackupFile while another process holds the writer: %v", err)
	}
	if revision <= first+50 {
		t.Fatalf("BackupFile at revision %d, the writer's fiftieth commit after %d before it", revision, first)
	}
	if after := newestRevision(t, path); after <= revision {
		t.Fatalf("the writer committed nothing after the copy's revision %d (newest %d): it did not go on", revision, after)
	}
	rest := stop()
	wrote := 0
	for _, line := range strings.Split(rest, "\n") {
		if fields := strings.Fields(line); len(fields) == 2 && fields[0] == "wrote" {
			wrote, _ = strconv.Atoi(fields[1])
		}
	}
	if uint64(wrote) <= revision-first {
		t.Fatalf("the writer said %q, the copy holds %d of its commits", rest, revision-first)
	}
	requireCopy(t, target, revision, before+int64(revision-first))

	// the writer gone: the file as it is, nothing left beside it
	beside := directoryNames(t, filepath.Dir(path))
	last := newestRevision(t, path)
	again := filepath.Join(t.TempDir(), "kitwork.db")
	if revision, err = BackupFile(ctx, path, again); err != nil || revision != last {
		t.Fatalf("BackupFile with no writer: revision %d, %v, want %d", revision, err, last)
	}
	requireCopy(t, again, revision, before+int64(wrote))
	if now := directoryNames(t, filepath.Dir(path)); !slices.Equal(now, beside) {
		t.Fatalf("BackupFile with no writer left %v beside the file, which had %v", now, beside)
	}

	// refused, leaving nothing
	refusedAt := filepath.Join(t.TempDir(), "refused")
	missing := filepath.Join(filepath.Dir(path), "missing.db")
	if _, err := BackupFile(ctx, missing, filepath.Join(refusedAt, "a.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("BackupFile of a file that is not there: %v", err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("BackupFile of a file that is not there made it: %v", err)
	}
	plain := filepath.Join(t.TempDir(), "plain.db")
	other, err := sql.Open("sqlite", dataSourceName(plain))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Exec(`CREATE TABLE things (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	_ = other.Close()
	if _, err := BackupFile(ctx, plain, filepath.Join(refusedAt, "b.db")); err == nil || !strings.Contains(err.Error(), "not a file KitDB wrote") {
		t.Fatalf("BackupFile of a file KitDB did not write: %v", err)
	}
	if names := directoryNames(t, refusedAt); len(names) != 0 {
		t.Fatalf("a refused backup left %v", names)
	}
}

// newestRevision reads the newest commit of the file at path through a connection of its own that changes nothing,
// leaving nothing beside the file when it is its last, as BackupFile's.
func newestRevision(t *testing.T, path string) uint64 {
	t.Helper()
	reader, err := sql.Open("sqlite", dataSourceName(path, "mode=rw", "_pragma=query_only(1)"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var revision uint64
	if err := reader.QueryRow(`SELECT coalesce(max(rev), 0) FROM kitwork_history`).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	return revision
}

// requireCopy opens the copy at target as a database: at revision, with rows rows, the ids from 1 without a gap.
func requireCopy(t *testing.T, target string, revision uint64, rows int64) {
	t.Helper()
	copied, err := Open(context.Background(), target, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Close()
	if got := copied.Revision(); got != revision {
		t.Fatalf("the copy opened at revision %d, BackupFile said %d", got, revision)
	}
	var count, highest int64
	if err := copied.readers.QueryRow(`SELECT count(*), coalesce(max(id), 0) FROM notes`).Scan(&count, &highest); err != nil {
		t.Fatal(err)
	}
	if count != rows || highest != rows {
		t.Fatalf("the copy at revision %d holds %d rows up to id %d, want the %d its commits wrote", revision, count, highest, rows)
	}
	var check string
	if err := copied.readers.QueryRow(`PRAGMA quick_check`).Scan(&check); err != nil || check != "ok" {
		t.Fatalf("the copy's quick_check: %q, %v", check, err)
	}
}

// directoryNames is what directory holds, sorted; nothing when it is not there.
func directoryNames(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// TestCopyFileCopiesAPlainSQLiteFile: CopyFile copies a SQLite file KitDB keeps no history in (a cluster's catalog,
// .catalog/kitwork.db, is one) as one read transaction sees it, while a writer goes on: the copy passes quick_check and
// holds the rows of the writer's commits up to a moment, without a gap. With no writer it leaves nothing beside the file;
// a file that is no SQLite file is refused, leaving nothing.
//
// CONTROL (13/11/2026): with CopyFile refusing a file of no history as BackupFile does, "CopyFile of a plain file: kitdb:
// backup of …: not a file KitDB wrote, it has no kitwork_history"; with empty pages written in place of the file's (a
// fault), CopyFile fails reading the copy back, as BackupFile does.
func TestCopyFileCopiesAPlainSQLiteFile(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "kitwork.db")
	writer, err := sql.Open("sqlite", dataSourceName(path, "_pragma=journal_mode(WAL)", "_txlock=immediate"))
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec(`CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan int)
	go func() {
		rows := 0
		for {
			select {
			case <-stop:
				done <- rows
				return
			default:
			}
			if _, err := writer.Exec(`INSERT INTO accounts (name) VALUES (?)`, strings.Repeat("a", 300)); err != nil {
				t.Error(err)
				done <- rows
				return
			}
			rows++
		}
	}()
	time.Sleep(20 * time.Millisecond)
	target := filepath.Join(t.TempDir(), "copy", "kitwork.db")
	err = CopyFile(ctx, path, target)
	close(stop)
	wrote := <-done
	if err != nil {
		t.Fatalf("CopyFile of a plain file: %v", err)
	}
	copied, err := sql.Open("sqlite", dataSourceName(target, "mode=ro"))
	if err != nil {
		t.Fatal(err)
	}
	var rows, highest int
	var check string
	if err := copied.QueryRow(`SELECT count(*), coalesce(max(id), 0) FROM accounts`).Scan(&rows, &highest); err != nil {
		t.Fatal(err)
	}
	if err := copied.QueryRow(`PRAGMA quick_check`).Scan(&check); err != nil || check != "ok" {
		t.Fatalf("the copy's quick_check: %q, %v", check, err)
	}
	_ = copied.Close()
	if rows == 0 || rows != highest || rows > wrote {
		t.Errorf("the copy holds %d rows up to id %d, the writer wrote %d", rows, highest, wrote)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	beside := directoryNames(t, directory)
	if err := CopyFile(ctx, path, filepath.Join(t.TempDir(), "again.db")); err != nil {
		t.Fatalf("CopyFile with no writer: %v", err)
	}
	if now := directoryNames(t, directory); !slices.Equal(now, beside) {
		t.Errorf("CopyFile with no writer left %v beside the file, which had %v", now, beside)
	}
	notSQLite := filepath.Join(directory, "notes.txt")
	if err := os.WriteFile(notSQLite, []byte(strings.Repeat("not a database ", 400)), 0o644); err != nil {
		t.Fatal(err)
	}
	refused := filepath.Join(t.TempDir(), "refused")
	if err := CopyFile(ctx, notSQLite, filepath.Join(refused, "notes.db")); err == nil {
		t.Errorf("a file of no SQLite copied: %v", err)
	}
	if names := directoryNames(t, refused); len(names) != 0 {
		t.Errorf("a refused copy left %v", names)
	}
}
