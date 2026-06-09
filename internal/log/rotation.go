package log

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// RotatingWriter implements io.WriteCloser with size-based log rotation.
type RotatingWriter struct {
	path       string
	maxBytes   int64
	maxBackups int
	compress   bool
	mu         sync.Mutex
	file       *os.File
	size       int64
}

// NewRotatingWriter creates a rotating file writer.
func NewRotatingWriter(path string, maxBytes int64, maxBackups int, compress bool) (*RotatingWriter, error) {
	rw := &RotatingWriter{
		path:       path,
		maxBytes:   maxBytes,
		maxBackups: maxBackups,
		compress:   compress,
	}

	if err := rw.openFile(); err != nil {
		return nil, err
	}

	return rw, nil
}

func (rw *RotatingWriter) openFile() error {
	dir := filepath.Dir(rw.path)
	os.MkdirAll(dir, 0755)

	f, err := os.OpenFile(rw.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	rw.file = f
	rw.size = info.Size()
	return nil
}

// Write implements io.Writer with rotation check.
func (rw *RotatingWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.size+int64(len(p)) > rw.maxBytes {
		if err := rw.rotate(); err != nil {
			// If rotation fails, still try to write
			_ = err
		}
	}

	n, err := rw.file.Write(p)
	rw.size += int64(n)
	return n, err
}

// Close closes the underlying file.
func (rw *RotatingWriter) Close() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.file != nil {
		return rw.file.Close()
	}
	return nil
}

func (rw *RotatingWriter) rotate() error {
	// Close current file
	rw.file.Close()

	// Shift existing backups: .3 → .4, .2 → .3, .1 → .2
	for i := rw.maxBackups - 1; i >= 1; i-- {
		src := rw.backupName(i)
		dst := rw.backupName(i + 1)
		os.Remove(dst)
		os.Rename(src, dst)
	}

	// Delete oldest if over maxBackups
	os.Remove(rw.backupName(rw.maxBackups + 1))

	// Current → .1. Compression is done synchronously and only removes the
	// source once the backup is durably written, so a crash or disk-full during
	// compression can never lose log data. Doing it inline (rather than in a
	// background goroutine) also avoids racing the next rotate()'s backup shift.
	if rw.compress {
		if err := compressTo(rw.path, rw.backupName(1)); err != nil {
			// Compression failed - fall back to an uncompressed rename so the data
			// is preserved rather than discarded.
			os.Rename(rw.path, fmt.Sprintf("%s.1", rw.path))
		} else {
			os.Remove(rw.path)
		}
	} else {
		os.Rename(rw.path, rw.backupName(1))
	}

	// Open new file
	rw.size = 0
	return rw.openFile()
}

func (rw *RotatingWriter) backupName(n int) string {
	if rw.compress {
		return fmt.Sprintf("%s.%d.gz", rw.path, n)
	}
	return fmt.Sprintf("%s.%d", rw.path, n)
}

// compressTo gzips srcPath into dstPath. It returns an error (and removes any
// partial output) if any step fails, so the caller can decide whether it is safe
// to delete the source. It never deletes the source itself.
func compressTo(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}

	gz := gzip.NewWriter(dst)
	if _, err := io.Copy(gz, src); err != nil {
		gz.Close()
		dst.Close()
		os.Remove(dstPath)
		return err
	}
	if err := gz.Close(); err != nil {
		dst.Close()
		os.Remove(dstPath)
		return err
	}
	if err := dst.Close(); err != nil {
		os.Remove(dstPath)
		return err
	}
	return nil
}

// ParseMaxSize converts a size string like "100MB", "1GB" to bytes.
func ParseMaxSize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	var multiplier int64 = 1

	switch {
	case strings.HasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	}

	var val int64
	fmt.Sscanf(s, "%d", &val)
	if val <= 0 {
		return 100 * 1024 * 1024 // default 100MB
	}
	return val * multiplier
}
