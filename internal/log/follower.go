package log

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultFollowPollInterval = 500 * time.Millisecond
	followAnchorSize          = 64
)

// Follower streams the current stdout and stderr log files for one process.
// It re-discovers instance files on every poll and keeps open descriptors long
// enough to drain writes that raced with a rename-based rotation.
type Follower struct {
	logDir       string
	level        string
	pollInterval time.Duration
}

type followedFile struct {
	file    *os.File
	offset  int64
	anchor  []byte
	pending []byte
}

type backlogLine struct {
	text      string
	timestamp time.Time
	path      string
	index     int
}

// NewFollower creates a follower for a process log directory. An error level
// follower reads stderr files only; an empty level combines stdout and stderr.
func NewFollower(logDir, level string, pollInterval time.Duration) *Follower {
	if pollInterval <= 0 {
		pollInterval = defaultFollowPollInterval
	}

	return &Follower{
		logDir:       logDir,
		level:        level,
		pollInterval: pollInterval,
	}
}

// Follow emits the last backlogLimit complete lines in chronological order,
// then blocks while emitting appended lines until the context is cancelled.
func (f *Follower) Follow(ctx context.Context, backlogLimit int, emit func(string) error) error {
	if backlogLimit < 0 {
		backlogLimit = 0
	}

	states := make(map[string]*followedFile)
	defer closeFollowedFiles(states)

	backlog, err := f.openInitialFiles(states, backlogLimit)
	if err != nil {
		return err
	}
	if backlogLimit > 0 && len(backlog) > backlogLimit {
		backlog = backlog[len(backlog)-backlogLimit:]
	}
	for _, line := range backlog {
		if err := emit(line.text); err != nil {
			return err
		}
	}

	ticker := time.NewTicker(f.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := f.poll(states, emit); err != nil {
				return err
			}
		}
	}
}

func (f *Follower) openInitialFiles(states map[string]*followedFile, backlogLimit int) ([]backlogLine, error) {
	paths, err := f.discoverPaths()
	if err != nil {
		return nil, err
	}

	var backlog []backlogLine
	for _, path := range paths {
		state, err := openFollowedFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		states[path] = state

		lines, err := state.readInitialLines(backlogLimit)
		if err != nil {
			return nil, fmt.Errorf("read log file %s: %w", path, err)
		}
		for index, line := range lines {
			backlog = append(backlog, backlogLine{
				text:      line,
				timestamp: lineTimestamp(line),
				path:      path,
				index:     index,
			})
		}
	}

	sort.SliceStable(backlog, func(i, j int) bool {
		left, right := backlog[i], backlog[j]
		if !left.timestamp.Equal(right.timestamp) {
			if left.timestamp.IsZero() {
				return true
			}
			if right.timestamp.IsZero() {
				return false
			}
			return left.timestamp.Before(right.timestamp)
		}
		if left.path != right.path {
			return left.path < right.path
		}
		return left.index < right.index
	})

	return backlog, nil
}

func (state *followedFile) readInitialLines(limit int) ([]string, error) {
	reader := bufio.NewReader(state.file)
	lines := make([]string, 0, limit)

	for {
		rawLine, err := reader.ReadString('\n')
		if len(rawLine) > 0 {
			if strings.HasSuffix(rawLine, "\n") {
				line := strings.TrimSuffix(strings.TrimSuffix(rawLine, "\n"), "\r")
				if line != "" && limit > 0 {
					if len(lines) == limit {
						copy(lines, lines[1:])
						lines[len(lines)-1] = line
					} else {
						lines = append(lines, line)
					}
				}
			} else {
				state.pending = append(state.pending, rawLine...)
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	offset, err := state.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	state.offset = offset
	if err := state.refreshAnchor(); err != nil {
		return nil, err
	}
	return lines, nil
}

func (f *Follower) poll(states map[string]*followedFile, emit func(string) error) error {
	paths, err := f.discoverPaths()
	if err != nil {
		return err
	}
	discovered := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		discovered[path] = struct{}{}
	}

	var removedPaths []string
	for path := range states {
		if _, ok := discovered[path]; !ok {
			removedPaths = append(removedPaths, path)
		}
	}
	sort.Strings(removedPaths)
	for _, path := range removedPaths {
		state := states[path]
		if err := emitFollowedLines(state, emit); err != nil {
			return err
		}
		_ = state.file.Close()
		delete(states, path)
	}

	for _, path := range paths {
		pathInfo, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("stat log file %s: %w", path, err)
		}

		state, exists := states[path]
		if !exists {
			state, err = openFollowedFile(path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return err
			}
			states[path] = state
			if err := emitFollowedLines(state, emit); err != nil {
				return err
			}
			continue
		}

		openInfo, err := state.file.Stat()
		if err != nil {
			return fmt.Errorf("stat open log file %s: %w", path, err)
		}
		if !os.SameFile(openInfo, pathInfo) {
			if err := emitFollowedLines(state, emit); err != nil {
				return err
			}
			_ = state.file.Close()

			state, err = openFollowedFile(path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					delete(states, path)
					continue
				}
				return err
			}
			states[path] = state
		}

		truncated, err := state.wasTruncated(pathInfo.Size())
		if err != nil {
			return fmt.Errorf("detect truncation for %s: %w", path, err)
		}
		if truncated {
			if _, err := state.file.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("rewind truncated log file %s: %w", path, err)
			}
			state.offset = 0
			state.anchor = nil
			state.pending = nil
		}

		if err := emitFollowedLines(state, emit); err != nil {
			return err
		}
	}

	return nil
}

func (f *Follower) discoverPaths() ([]string, error) {
	entries, err := os.ReadDir(f.logDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read log directory: %w", err)
	}

	var paths []string
	for _, entry := range entries {
		if entry.IsDir() || !f.includes(entry.Name()) {
			continue
		}
		paths = append(paths, filepath.Join(f.logDir, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

func (f *Follower) includes(name string) bool {
	isStdout := name == "current.log" || isInstanceLog(name, ".log")
	isStderr := name == "error.log" || isInstanceLog(name, ".error.log")
	if f.level == "error" {
		return isStderr
	}
	return isStdout || isStderr
}

func isInstanceLog(name, suffix string) bool {
	if !strings.HasPrefix(name, "instance-") || !strings.HasSuffix(name, suffix) {
		return false
	}
	instance := strings.TrimSuffix(strings.TrimPrefix(name, "instance-"), suffix)
	if instance == "" {
		return false
	}
	for _, char := range instance {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func openFollowedFile(path string) (*followedFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", path, err)
	}
	return &followedFile{file: file}, nil
}

func emitFollowedLines(state *followedFile, emit func(string) error) error {
	lines, err := state.readCompleteLines()
	if err != nil {
		return err
	}
	for _, line := range lines {
		if err := emit(line); err != nil {
			return err
		}
	}
	return nil
}

func (state *followedFile) readCompleteLines() ([]string, error) {
	data, err := io.ReadAll(state.file)
	if err != nil {
		return nil, err
	}
	state.offset += int64(len(data))
	data = append(state.pending, data...)
	state.pending = nil

	var lines []string
	for {
		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			state.pending = append(state.pending, data...)
			break
		}
		line := strings.TrimSuffix(string(data[:newline]), "\r")
		if line != "" {
			lines = append(lines, line)
		}
		data = data[newline+1:]
	}

	if err := state.refreshAnchor(); err != nil {
		return nil, err
	}
	return lines, nil
}

func (state *followedFile) wasTruncated(size int64) (bool, error) {
	if size < state.offset {
		return true, nil
	}
	if len(state.anchor) == 0 || state.offset < int64(len(state.anchor)) {
		return false, nil
	}

	actual := make([]byte, len(state.anchor))
	if _, err := state.file.ReadAt(actual, state.offset-int64(len(actual))); err != nil {
		return false, err
	}
	return !bytes.Equal(actual, state.anchor), nil
}

func (state *followedFile) refreshAnchor() error {
	anchorLength := int64(followAnchorSize)
	if state.offset < anchorLength {
		anchorLength = state.offset
	}
	if anchorLength == 0 {
		state.anchor = nil
		return nil
	}

	anchor := make([]byte, anchorLength)
	if _, err := state.file.ReadAt(anchor, state.offset-anchorLength); err != nil {
		return err
	}
	state.anchor = anchor
	return nil
}

func closeFollowedFiles(states map[string]*followedFile) {
	for _, state := range states {
		_ = state.file.Close()
	}
}

func lineTimestamp(line string) time.Time {
	if len(line) >= 20 {
		if parsed, err := time.Parse("2006-01-02T15:04:05Z", line[:20]); err == nil {
			return parsed
		}
	}
	if len(line) >= 25 {
		if parsed, err := time.Parse(time.RFC3339, line[:25]); err == nil {
			return parsed
		}
	}
	return time.Time{}
}
