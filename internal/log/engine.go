package log

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Entry represents a single structured log entry.
type Entry struct {
	Timestamp time.Time         `json:"timestamp"`
	Level     string            `json:"level"`
	App       string            `json:"app"`
	Instance  int               `json:"instance"`
	PID       int               `json:"pid"`
	Type      string            `json:"type"` // stdout, stderr
	Message   string            `json:"message"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Filter defines criteria for log retrieval.
type Filter struct {
	Level  string
	Since  time.Time
	Until  time.Time
	Lines  int
	Follow bool
}

// Engine handles structured logging, rotation, and retrieval.
type Engine struct {
	baseDir string
}

// NewEngine creates a new log engine.
func NewEngine(baseDir string) *Engine {
	return &Engine{baseDir: baseDir}
}

// GetLogs retrieves log entries for an app with optional filtering.
func (e *Engine) GetLogs(appName string, filter Filter) ([]Entry, error) {
	logPath := filepath.Join(e.baseDir, "apps", appName, "current.log")

	file, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	defer file.Close()

	var entries []Entry
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB line buffer

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		entry := parseLine(line, appName)

		// Apply filters
		if filter.Level != "" && entry.Level != filter.Level {
			continue
		}
		if !filter.Since.IsZero() && entry.Timestamp.Before(filter.Since) {
			continue
		}
		if !filter.Until.IsZero() && entry.Timestamp.After(filter.Until) {
			continue
		}

		entries = append(entries, entry)
	}

	// Apply line limit (take last N)
	if filter.Lines > 0 && len(entries) > filter.Lines {
		entries = entries[len(entries)-filter.Lines:]
	}

	return entries, scanner.Err()
}

// GetErrorLogs retrieves only error-level logs.
func (e *Engine) GetErrorLogs(appName string, lines int) ([]Entry, error) {
	return e.GetLogs(appName, Filter{Level: "error", Lines: lines})
}

// ParseLine parses a single log line into a structured Entry.
func (e *Engine) ParseLine(line, appName string) Entry {
	return parseLine(line, appName)
}

// LogDir returns the log directory for an app.
func (e *Engine) LogDir(appName string) string {
	return filepath.Join(e.baseDir, "apps", appName)
}

// parseLine attempts to parse a log line into a structured Entry.
// Supports: JSON logs (pino/winston), Laravel logs, plain text.
func parseLine(line, appName string) Entry {
	entry := Entry{
		Timestamp: time.Now(),
		Level:     "info",
		App:       appName,
		Type:      "stdout",
		Message:   line,
	}

	// Try JSON parse first (pino, winston, bunyan, etc.)
	if strings.HasPrefix(line, "{") {
		if parsed := parseJSONLog(line); parsed != nil {
			entry.Timestamp = parsed.Timestamp
			entry.Level = parsed.Level
			entry.Message = parsed.Message
			entry.Metadata = parsed.Metadata
			return entry
		}
	}

	// Try Laravel log format: [YYYY-MM-DD HH:MM:SS] env.LEVEL: message
	if parsed := parseLaravelLog(line); parsed != nil {
		entry.Timestamp = parsed.Timestamp
		entry.Level = parsed.Level
		entry.Message = parsed.Message
		return entry
	}

	// Try common log prefixes for level detection
	entry.Level = detectLevel(line)

	return entry
}

// parseJSONLog attempts to parse a JSON log line.
func parseJSONLog(line string) *Entry {
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return nil
	}

	entry := &Entry{
		Timestamp: time.Now(),
		Level:     "info",
	}

	// Extract level - try common field names
	for _, key := range []string{"level", "lvl", "severity"} {
		if v, ok := raw[key]; ok {
			entry.Level = normalizeLevel(fmt.Sprintf("%v", v))
			break
		}
	}

	// Extract message
	for _, key := range []string{"msg", "message", "text"} {
		if v, ok := raw[key]; ok {
			entry.Message = fmt.Sprintf("%v", v)
			break
		}
	}

	// Extract timestamp
	for _, key := range []string{"time", "timestamp", "ts", "@timestamp"} {
		if v, ok := raw[key]; ok {
			switch tv := v.(type) {
			case string:
				if t, err := time.Parse(time.RFC3339Nano, tv); err == nil {
					entry.Timestamp = t
				}
			case float64:
				// Unix timestamp (seconds or milliseconds)
				if tv > 1e12 {
					entry.Timestamp = time.UnixMilli(int64(tv))
				} else {
					entry.Timestamp = time.Unix(int64(tv), 0)
				}
			}
			break
		}
	}

	// Collect remaining fields as metadata
	entry.Metadata = make(map[string]string)
	skipKeys := map[string]bool{
		"level": true, "lvl": true, "severity": true,
		"msg": true, "message": true, "text": true,
		"time": true, "timestamp": true, "ts": true, "@timestamp": true,
	}
	for k, v := range raw {
		if !skipKeys[k] {
			entry.Metadata[k] = fmt.Sprintf("%v", v)
		}
	}

	return entry
}

var laravelLogRegex = regexp.MustCompile(`^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?\d*)\]\s+\w+\.(\w+):\s+(.*)$`)

// parseLaravelLog parses a Laravel log format line.
func parseLaravelLog(line string) *Entry {
	matches := laravelLogRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	ts := matches[1]
	level := strings.ToLower(matches[2])
	message := matches[3]

	t, err := time.Parse("2006-01-02 15:04:05", ts)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05.000000", ts)
		if err != nil {
			t = time.Now()
		}
	}

	return &Entry{
		Timestamp: t,
		Level:     normalizeLevel(level),
		Message:   message,
	}
}

// detectLevel detects log level from common prefixes/patterns.
func detectLevel(line string) string {
	lower := strings.ToLower(line)

	switch {
	case strings.Contains(lower, "error") || strings.Contains(lower, "fatal") ||
		strings.Contains(lower, "panic") || strings.Contains(lower, "exception"):
		return "error"
	case strings.Contains(lower, "warn"):
		return "warn"
	case strings.Contains(lower, "debug") || strings.Contains(lower, "trace"):
		return "debug"
	default:
		return "info"
	}
}

// normalizeLevel normalizes level strings to standard values.
func normalizeLevel(level string) string {
	switch strings.ToLower(level) {
	case "error", "err", "fatal", "critical", "emergency", "alert", "50", "60":
		return "error"
	case "warn", "warning", "40":
		return "warn"
	case "info", "notice", "30":
		return "info"
	case "debug", "trace", "20", "10":
		return "debug"
	default:
		return "info"
	}
}
