package log

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const followTestPollInterval = 5 * time.Millisecond

func TestFollowerStreamsBacklogAndNewLinesExactlyOnce(t *testing.T) {
	logDir := t.TempDir()
	writeFollowTestFile(t, filepath.Join(logDir, "current.log"),
		"2026-09-03T10:00:00Z old-1\n"+
			"2026-09-03T10:00:01Z old-2\n"+
			"2026-09-03T10:00:02Z old-3\n")

	lines, cancel, done := startFollowTest(t, logDir, "", 2)
	defer stopFollowTest(t, cancel, done)

	wantFollowLines(t, lines,
		"2026-09-03T10:00:01Z old-2",
		"2026-09-03T10:00:02Z old-3",
	)

	if err := appendFollowTestFile(filepath.Join(logDir, "current.log"),
		"2026-09-03T10:00:03Z new-1\n2026-09-03T10:00:04Z new-2\n"); err != nil {
		t.Fatalf("append log: %v", err)
	}

	wantFollowLines(t, lines,
		"2026-09-03T10:00:03Z new-1",
		"2026-09-03T10:00:04Z new-2",
	)
	assertNoFollowLine(t, lines)
}

func TestFollowerCombinesStdoutAndStderrWithoutLoss(t *testing.T) {
	logDir := t.TempDir()
	writeFollowTestFile(t, filepath.Join(logDir, "current.log"), "")
	writeFollowTestFile(t, filepath.Join(logDir, "error.log"), "")

	lines, cancel, done := startFollowTest(t, logDir, "", 10)
	defer stopFollowTest(t, cancel, done)

	var writers sync.WaitGroup
	writeErrors := make(chan error, 2)
	writers.Add(2)
	go func() {
		defer writers.Done()
		writeErrors <- appendFollowTestFile(filepath.Join(logDir, "current.log"), "2026-09-03T10:01:00Z stdout\n")
	}()
	go func() {
		defer writers.Done()
		writeErrors <- appendFollowTestFile(filepath.Join(logDir, "error.log"), "2026-09-03T10:01:01Z stderr\n")
	}()
	writers.Wait()
	close(writeErrors)
	for err := range writeErrors {
		if err != nil {
			t.Fatalf("append concurrent log: %v", err)
		}
	}

	got := map[string]bool{}
	for range 2 {
		got[wantFollowLine(t, lines)] = true
	}
	if !got["2026-09-03T10:01:00Z stdout"] || !got["2026-09-03T10:01:01Z stderr"] {
		t.Fatalf("combined stream = %#v, want stdout and stderr", got)
	}
}

func TestFollowerContinuesAfterRotation(t *testing.T) {
	logDir := t.TempDir()
	currentPath := filepath.Join(logDir, "current.log")
	writeFollowTestFile(t, currentPath, "2026-09-03T10:02:00Z initial\n")

	lines, cancel, done := startFollowTest(t, logDir, "", 10)
	defer stopFollowTest(t, cancel, done)
	wantFollowLines(t, lines, "2026-09-03T10:02:00Z initial")

	if err := appendFollowTestFile(currentPath, "2026-09-03T10:02:01Z before-rotation\n"); err != nil {
		t.Fatalf("append log: %v", err)
	}
	if err := os.Rename(currentPath, currentPath+".1"); err != nil {
		t.Fatalf("rotate log: %v", err)
	}
	writeFollowTestFile(t, currentPath, "2026-09-03T10:02:02Z after-rotation\n")

	wantFollowLines(t, lines,
		"2026-09-03T10:02:01Z before-rotation",
		"2026-09-03T10:02:02Z after-rotation",
	)
	assertNoFollowLine(t, lines)
}

func TestFollowerContinuesAfterTruncateAndRegrow(t *testing.T) {
	logDir := t.TempDir()
	currentPath := filepath.Join(logDir, "current.log")
	writeFollowTestFile(t, currentPath, "2026-09-03T10:03:00Z original-line-with-long-content\n")

	lines, cancel, done := startFollowTest(t, logDir, "", 10)
	defer stopFollowTest(t, cancel, done)
	wantFollowLines(t, lines, "2026-09-03T10:03:00Z original-line-with-long-content")

	writeFollowTestFile(t, currentPath,
		"2026-09-03T10:03:01Z replacement-line-after-truncate-that-is-longer-than-the-original\n")

	wantFollowLines(t, lines,
		"2026-09-03T10:03:01Z replacement-line-after-truncate-that-is-longer-than-the-original",
	)
	assertNoFollowLine(t, lines)
}

func TestFollowerDiscoversInstanceFilesCreatedAndRemovedWhileRunning(t *testing.T) {
	logDir := t.TempDir()
	writeFollowTestFile(t, filepath.Join(logDir, "current.log"), "")

	lines, cancel, done := startFollowTest(t, logDir, "", 10)
	defer stopFollowTest(t, cancel, done)

	instancePath := filepath.Join(logDir, "instance-7.log")
	writeFollowTestFile(t, instancePath, "2026-09-03T10:04:00Z instance-created\n")
	wantFollowLines(t, lines, "2026-09-03T10:04:00Z instance-created")

	if err := os.Remove(instancePath); err != nil {
		t.Fatalf("remove instance log: %v", err)
	}
	time.Sleep(3 * followTestPollInterval)
	writeFollowTestFile(t, instancePath, "2026-09-03T10:04:01Z instance-recreated\n")
	wantFollowLines(t, lines, "2026-09-03T10:04:01Z instance-recreated")
}

func TestFollowerStopsPromptlyWhenContextIsCancelled(t *testing.T) {
	logDir := t.TempDir()
	writeFollowTestFile(t, filepath.Join(logDir, "current.log"), "")

	_, cancel, done := startFollowTest(t, logDir, "", 10)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not stop after cancellation")
	}

	if err := os.RemoveAll(logDir); err != nil {
		t.Fatalf("remove log directory after cancellation: %v", err)
	}
}

func startFollowTest(t *testing.T, logDir, level string, backlog int) (<-chan string, context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	lines := make(chan string, 32)
	done := make(chan error, 1)
	follower := NewFollower(logDir, level, followTestPollInterval)

	go func() {
		done <- follower.Follow(ctx, backlog, func(line string) error {
			lines <- line
			return nil
		})
	}()

	return lines, cancel, done
}

func stopFollowTest(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Follow() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not stop")
	}
}

func wantFollowLines(t *testing.T, lines <-chan string, want ...string) {
	t.Helper()
	for _, expected := range want {
		if got := wantFollowLine(t, lines); got != expected {
			t.Fatalf("follow line = %q, want %q", got, expected)
		}
	}
}

func wantFollowLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	select {
	case line := <-lines:
		return line
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for followed log line")
		return ""
	}
}

func assertNoFollowLine(t *testing.T, lines <-chan string) {
	t.Helper()
	select {
	case line := <-lines:
		t.Fatalf("unexpected duplicate follow line %q", line)
	case <-time.After(5 * followTestPollInterval):
	}
}

func writeFollowTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func appendFollowTestFile(path, content string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(content); err != nil {
		return err
	}
	return nil
}
