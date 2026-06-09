package log

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestRotateCompressPreservesData verifies that size-based rotation with
// compression enabled produces a readable .gz backup whose decompressed content
// matches what was written, and that no log data is lost across rotations.
func TestRotateCompressPreservesData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	// Small maxBytes so each write triggers a rotation.
	rw, err := NewRotatingWriter(path, 16, 3, true)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	// First chunk goes into the live file, then a second write forces rotation
	// of the first chunk into app.log.1.gz.
	chunk1 := []byte("AAAAAAAAAAAAAAAAAAAA\n") // > 16 bytes
	chunk2 := []byte("BBBBBBBBBBBBBBBBBBBB\n")
	if _, err := rw.Write(chunk1); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if _, err := rw.Write(chunk2); err != nil {
		t.Fatalf("write2: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The rotated backup must exist as a .gz (compression is synchronous now).
	gzPath := path + ".1.gz"
	if _, err := os.Stat(gzPath); err != nil {
		t.Fatalf("expected compressed backup %s: %v", gzPath, err)
	}

	// The uncompressed source must NOT linger once it was durably compressed.
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Errorf("uncompressed backup app.log.1 should have been removed, stat err=%v", err)
	}

	// Decompress and confirm chunk1 survived intact.
	got := readGzip(t, gzPath)
	if !bytes.Contains(got, chunk1) {
		t.Errorf("compressed backup missing original data: got %q want to contain %q", got, chunk1)
	}
}

// TestRotateNoCompressKeepsPlainBackup verifies the uncompressed path still
// produces a plain .1 backup.
func TestRotateNoCompressKeepsPlainBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	rw, err := NewRotatingWriter(path, 16, 3, false)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if _, err := rw.Write([]byte("AAAAAAAAAAAAAAAAAAAA\n")); err != nil {
		t.Fatalf("write1: %v", err)
	}
	if _, err := rw.Write([]byte("BBBBBBBBBBBBBBBBBBBB\n")); err != nil {
		t.Fatalf("write2: %v", err)
	}
	rw.Close()

	if _, err := os.Stat(path + ".1"); err != nil {
		t.Errorf("expected plain backup app.log.1: %v", err)
	}
}

func readGzip(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open gz: %v", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader (corrupt backup?): %v", err)
	}
	defer gz.Close()
	data, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gz: %v", err)
	}
	return data
}
