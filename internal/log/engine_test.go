package log

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseJSONLog_Pino(t *testing.T) {
	line := `{"level":30,"time":1711108200000,"msg":"Server listening on port 3000","pid":12345}`
	entry := parseLine(line, "test-app")

	if entry.Level != "info" {
		t.Errorf("Level = %s, want info", entry.Level)
	}
	if entry.Message != "Server listening on port 3000" {
		t.Errorf("Message = %s", entry.Message)
	}
	if entry.App != "test-app" {
		t.Errorf("App = %s, want test-app", entry.App)
	}
}

func TestParseJSONLog_Winston(t *testing.T) {
	line := `{"level":"error","message":"Connection refused","timestamp":"2026-03-22T14:30:00.123Z"}`
	entry := parseLine(line, "test-app")

	if entry.Level != "error" {
		t.Errorf("Level = %s, want error", entry.Level)
	}
	if entry.Message != "Connection refused" {
		t.Errorf("Message = %s", entry.Message)
	}
}

func TestParseLaravelLog(t *testing.T) {
	line := `[2026-03-22 14:30:00] production.ERROR: SQLSTATE[42S02]: Base table or view not found`
	entry := parseLine(line, "laravel-app")

	if entry.Level != "error" {
		t.Errorf("Level = %s, want error", entry.Level)
	}
	if entry.Message != "SQLSTATE[42S02]: Base table or view not found" {
		t.Errorf("Message = %s", entry.Message)
	}
	if entry.Timestamp.Year() != 2026 {
		t.Errorf("Year = %d, want 2026", entry.Timestamp.Year())
	}
}

func TestParseLaravelLogInfo(t *testing.T) {
	line := `[2026-03-22 10:00:00] local.INFO: User logged in {"user_id":42}`
	entry := parseLine(line, "app")

	if entry.Level != "info" {
		t.Errorf("Level = %s, want info", entry.Level)
	}
}

func TestParsePlainText(t *testing.T) {
	line := `listening on port 3000`
	entry := parseLine(line, "app")

	if entry.Level != "info" {
		t.Errorf("Level = %s, want info (default)", entry.Level)
	}
	if entry.Message != line {
		t.Errorf("Message = %s, want original line", entry.Message)
	}
}

func TestParsePlainTextError(t *testing.T) {
	line := `Error: ENOENT: no such file or directory`
	entry := parseLine(line, "app")

	if entry.Level != "error" {
		t.Errorf("Level = %s, want error", entry.Level)
	}
}

func TestDetectLevel(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Error: something went wrong", "error"},
		{"FATAL: process crashed", "error"},
		{"panic: runtime error", "error"},
		{"Warning: deprecated function", "warn"},
		{"[WARN] low disk space", "warn"},
		{"DEBUG: variable dump", "debug"},
		{"Server started successfully", "info"},
	}

	for _, tt := range tests {
		got := detectLevel(tt.input)
		if got != tt.want {
			t.Errorf("detectLevel(%q) = %s, want %s", tt.input, got, tt.want)
		}
	}
}

func TestNormalizeLevel(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"error", "error"},
		{"err", "error"},
		{"fatal", "error"},
		{"critical", "error"},
		{"emergency", "error"},
		{"50", "error"},
		{"warn", "warn"},
		{"warning", "warn"},
		{"40", "warn"},
		{"info", "info"},
		{"notice", "info"},
		{"30", "info"},
		{"debug", "debug"},
		{"trace", "debug"},
		{"20", "debug"},
		{"unknown", "info"},
	}

	for _, tt := range tests {
		got := normalizeLevel(tt.input)
		if got != tt.want {
			t.Errorf("normalizeLevel(%q) = %s, want %s", tt.input, got, tt.want)
		}
	}
}

func TestGetLogs(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine(dir)

	// Create test log file
	appDir := dir + "/apps/test-app"
	if err := createTestLogFile(appDir); err != nil {
		t.Fatalf("create test log: %v", err)
	}

	entries, err := engine.GetLogs("test-app", Filter{Lines: 10})
	if err != nil {
		t.Fatalf("get logs: %v", err)
	}

	if len(entries) == 0 {
		t.Error("expected log entries")
	}
}

func TestGetLogsWithLevelFilter(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine(dir)

	appDir := dir + "/apps/test-app"
	createTestLogFile(appDir)

	entries, err := engine.GetLogs("test-app", Filter{Level: "error"})
	if err != nil {
		t.Fatalf("get logs: %v", err)
	}

	for _, e := range entries {
		if e.Level != "error" {
			t.Errorf("got level %s, want error", e.Level)
		}
	}
}

func TestGetLogsNotFound(t *testing.T) {
	engine := NewEngine(t.TempDir())

	_, err := engine.GetLogs("nonexistent", Filter{})
	if err == nil {
		t.Error("expected error for missing log file")
	}
}

func TestGetLogsWithSinceFilter(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine(dir)
	appDir := dir + "/apps/test-app"
	createTestLogFile(appDir)

	future := time.Now().Add(1 * time.Hour)
	entries, err := engine.GetLogs("test-app", Filter{Since: future})
	if err != nil {
		t.Fatalf("get logs: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("expected 0 entries for future since filter, got %d", len(entries))
	}
}

func TestIsTabContinuation(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		// UTC timestamp with tab marker
		{"2026-04-16T10:00:00Z \t  \"sent\": \"TRY\",", true},
		{"2026-04-16T10:00:00Z \t}", true},
		// Normal line (no tab)
		{"2026-04-16T10:00:00Z [INFO] Server started", false},
		// RFC3339 with offset and tab marker
		{"2026-04-16T10:00:00+03:00 \t  \"key\": \"val\"", true},
		// Too short
		{"short", false},
		// No timestamp pattern
		{"not-a-timestamp-at-all here", false},
	}

	for _, tt := range tests {
		got := IsTabContinuation(tt.line)
		if got != tt.want {
			t.Errorf("IsTabContinuation(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}

func TestExtractContinuationContent(t *testing.T) {
	tests := []struct {
		line string
		want string
	}{
		{"2026-04-16T10:00:00Z \t  \"sent\": \"TRY\",", "  \"sent\": \"TRY\","},
		{"2026-04-16T10:00:00Z \t}", "}"},
		{"2026-04-16T10:00:00+03:00 \t  nested", "  nested"},
	}

	for _, tt := range tests {
		got := ExtractContinuationContent(tt.line)
		if got != tt.want {
			t.Errorf("ExtractContinuationContent(%q) = %q, want %q", tt.line, got, tt.want)
		}
	}
}

func TestGetLogsMultiLineMerge(t *testing.T) {
	dir := t.TempDir()
	engine := NewEngine(dir)

	appDir := dir + "/apps/test-app"
	if err := os.MkdirAll(appDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Simulate timestampWriter output with continuation lines
	logContent := "2026-04-16T10:00:00Z [CURRENCY] Backend response: {\n" +
		"2026-04-16T10:00:00Z \t  \"sent\": \"TRY\",\n" +
		"2026-04-16T10:00:00Z \t  \"price\": 4999\n" +
		"2026-04-16T10:00:00Z \t}\n" +
		"2026-04-16T10:00:01Z Next log entry\n"

	if err := os.WriteFile(appDir+"/current.log", []byte(logContent), 0644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	entries, err := engine.GetLogs("test-app", Filter{Lines: 100})
	if err != nil {
		t.Fatalf("get logs: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (multi-line merged), got %d", len(entries))
	}

	// First entry should have merged message
	expected := "[CURRENCY] Backend response: {\n  \"sent\": \"TRY\",\n  \"price\": 4999\n}"
	if entries[0].Message != expected {
		t.Errorf("merged message:\ngot:  %q\nwant: %q", entries[0].Message, expected)
	}

	// Second entry should be separate
	if entries[1].Message != "Next log entry" {
		t.Errorf("second entry: got %q, want %q", entries[1].Message, "Next log entry")
	}
}

func createTestLogFile(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	lines := []string{
		`{"level":30,"time":1711108200000,"msg":"Server started","pid":1234}`,
		`[2026-03-22 14:30:01] production.INFO: Request handled`,
		`[2026-03-22 14:30:02] production.ERROR: Connection timeout`,
		`plain text log line`,
		`Error: something broke`,
	}
	return os.WriteFile(dir+"/current.log", []byte(strings.Join(lines, "\n")+"\n"), 0644)
}
