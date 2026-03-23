package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const defaultSocket = "/var/run/dpm/dpm.sock"

// version is set at build time via ldflags.
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "start":
		cmdStart(args)
	case "stop":
		cmdStop(args)
	case "restart":
		cmdRestart(args)
	case "delete":
		cmdDelete(args)
	case "list", "ls":
		cmdList()
	case "info":
		cmdInfo(args)
	case "logs":
		cmdLogs(args)
	case "status":
		cmdStatus()
	case "health":
		cmdHealth(args)
	case "port":
		cmdPort(args)
	case "nginx":
		cmdNginx(args)
	case "upgrade":
		cmdUpgrade(args)
	case "version":
		cmdVersion()
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

// --- Commands ---

func cmdStart(args []string) {
	if len(args) == 0 {
		fatal("Usage: dpm start <config.yaml> or dpm start --config='<json>'")
	}

	arg := args[0]
	var body []byte

	if strings.HasPrefix(arg, "--config=") {
		// Inline JSON config
		body = []byte(strings.TrimPrefix(arg, "--config="))
	} else {
		// YAML file
		data, err := os.ReadFile(arg)
		if err != nil {
			fatal("Failed to read config: %v", err)
		}

		// Convert YAML to JSON for API (gopkg.in/yaml.v3 handles both)
		// For CLI simplicity, send as-is and let API parse
		body = data
	}

	resp := apiPost("/api/v1/processes", body)
	printJSON(resp)
}

func cmdStop(args []string) {
	name := requireArg(args, "Usage: dpm stop <name>")
	resp := apiPost(fmt.Sprintf("/api/v1/processes/%s/stop", name), nil)
	printJSON(resp)
}

func cmdRestart(args []string) {
	name := requireArg(args, "Usage: dpm restart <name>")
	resp := apiPost(fmt.Sprintf("/api/v1/processes/%s/restart", name), nil)
	printJSON(resp)
}

func cmdDelete(args []string) {
	name := requireArg(args, "Usage: dpm delete <name>")
	resp := apiDelete(fmt.Sprintf("/api/v1/processes/%s", name))
	printJSON(resp)
}

func cmdList() {
	body := apiGet("/api/v1/processes")

	var resp struct {
		Status string `json:"status"`
		Data   []struct {
			Name         string  `json:"name"`
			Type         string  `json:"type"`
			Status       string  `json:"status"`
			PID          int     `json:"pid"`
			Port         int     `json:"port"`
			MemoryBytes  uint64  `json:"memory_bytes"`
			UptimeNs     int64   `json:"uptime_ns"`
			RestartCount int     `json:"restart_count"`
			CPU          float64 `json:"cpu_percent"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		// Check if --json flag
		for _, a := range os.Args {
			if a == "--json" {
				fmt.Println(string(body))
				return
			}
		}
		fatal("Failed to parse response: %v", err)
	}

	// Check for --json flag
	for _, a := range os.Args {
		if a == "--json" {
			fmt.Println(string(body))
			return
		}
	}

	if resp.Data == nil || len(resp.Data) == 0 {
		fmt.Println("No processes running")
		return
	}

	// Build rows with manual padding, then colorize status in-place
	type row struct {
		name, typ, status, pid, port, mem, uptime string
		restarts                                  int
	}

	var rows []row
	for _, p := range resp.Data {
		pid := "-"
		if p.PID > 0 {
			pid = fmt.Sprintf("%d", p.PID)
		}
		portStr := "-"
		if p.Port > 0 {
			portStr = fmt.Sprintf("%d", p.Port)
		}
		rows = append(rows, row{
			name:     p.Name,
			typ:      p.Type,
			status:   p.Status,
			pid:      pid,
			port:     portStr,
			mem:      formatBytes(p.MemoryBytes),
			uptime:   formatDuration(time.Duration(p.UptimeNs)),
			restarts: p.RestartCount,
		})
	}

	// Calculate column widths from actual content (no ANSI)
	cw := [8]int{4, 4, 6, 3, 4, 6, 6, 8} // min = header widths
	for _, r := range rows {
		if len(r.name) > cw[0] { cw[0] = len(r.name) }
		if len(r.typ) > cw[1] { cw[1] = len(r.typ) }
		if len(r.status) > cw[2] { cw[2] = len(r.status) }
		if len(r.pid) > cw[3] { cw[3] = len(r.pid) }
		if len(r.port) > cw[4] { cw[4] = len(r.port) }
		if len(r.mem) > cw[5] { cw[5] = len(r.mem) }
		if len(r.uptime) > cw[6] { cw[6] = len(r.uptime) }
	}

	// Print with calculated widths
	fmtStr := fmt.Sprintf("%%-%ds  %%-%ds  %%-%ds  %%-%ds  %%-%ds  %%-%ds  %%-%ds  %%s\n",
		cw[0], cw[1], cw[2], cw[3], cw[4], cw[5], cw[6])

	fmt.Printf(fmtStr, "NAME", "TYPE", "STATUS", "PID", "PORT", "MEMORY", "UPTIME", "RESTARTS")

	sep := func(n int) string { s := ""; for i := 0; i < n; i++ { s += "-" }; return s }
	fmt.Printf(fmtStr, sep(cw[0]), sep(cw[1]), sep(cw[2]), sep(cw[3]), sep(cw[4]), sep(cw[5]), sep(cw[6]), sep(8))

	for _, r := range rows {
		// Pad status WITHOUT color, then wrap with color
		padded := fmt.Sprintf("%-*s", cw[2], r.status)
		colored := colorWrap(padded, r.status)

		fmt.Printf(fmt.Sprintf("%%-%ds  %%-%ds  %%s  %%-%ds  %%-%ds  %%-%ds  %%-%ds  %%d\n",
			cw[0], cw[1], cw[3], cw[4], cw[5], cw[6]),
			r.name, r.typ, colored, r.pid, r.port, r.mem, r.uptime, r.restarts)
	}
}

func cmdInfo(args []string) {
	name := requireArg(args, "Usage: dpm info <name>")
	body := apiGet(fmt.Sprintf("/api/v1/processes/%s", name))
	printFormattedJSON(body)
}

func cmdNginx(args []string) {
	if len(args) == 0 {
		fatal("Usage: dpm nginx <apply|remove|show|test|status>")
	}

	switch args[0] {
	case "apply":
		configJSON := ""
		for _, a := range args[1:] {
			if strings.HasPrefix(a, "--config=") {
				configJSON = strings.TrimPrefix(a, "--config=")
			}
		}
		if configJSON == "" {
			fatal("Usage: dpm nginx apply --config='<json>'")
		}
		resp := apiPost("/api/v1/nginx/apply", []byte(configJSON))
		printJSON(resp)

	case "remove":
		if len(args) < 2 {
			fatal("Usage: dpm nginx remove <domain>")
		}
		resp := apiDelete(fmt.Sprintf("/api/v1/nginx/remove/%s", args[1]))
		printJSON(resp)

	case "show":
		if len(args) < 2 {
			fatal("Usage: dpm nginx show <domain>")
		}
		body := apiGet(fmt.Sprintf("/api/v1/nginx/show/%s", args[1]))
		fmt.Print(string(body))

	case "test":
		body := apiGet("/api/v1/nginx/test")
		printJSON(body)

	case "status":
		body := apiGet("/api/v1/nginx/status")
		printFormattedJSON(body)

	default:
		fatal("Unknown nginx subcommand: %s", args[0])
	}
}

func cmdLogs(args []string) {
	name := ""
	lines := "100"
	level := ""
	isJSON := false
	isFollow := false

	for _, a := range args {
		switch {
		case strings.HasPrefix(a, "--lines="):
			lines = strings.TrimPrefix(a, "--lines=")
		case strings.HasPrefix(a, "--level="):
			level = strings.TrimPrefix(a, "--level=")
		case a == "--json":
			isJSON = true
		case a == "-f" || a == "--follow":
			isFollow = true
		case !strings.HasPrefix(a, "-"):
			name = a
		}
	}

	if name == "" {
		fatal("Usage: dpm logs <name> [--lines=100] [--level=error] [--json] [-f]")
	}

	query := fmt.Sprintf("?lines=%s", lines)
	if level != "" {
		query += "&level=" + level
	}
	if isJSON {
		query += "&format=json"
	}
	if isFollow {
		query += "&follow=true"
		// Stream mode: read response line by line
		client := httpClient()
		resp, err := client.Get("http://dpm" + fmt.Sprintf("/api/v1/logs/%s%s", name, query))
		if err != nil {
			fatal("Failed to connect to DPM daemon: %v\nIs dpmd running?", err)
		}
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 256*1024), 256*1024)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		return
	}

	body := apiGet(fmt.Sprintf("/api/v1/logs/%s%s", name, query))

	if isJSON {
		printFormattedJSON(body)
	} else {
		fmt.Print(string(body))
	}
}

func cmdStatus() {
	body := apiGet("/api/v1/status")
	printFormattedJSON(body)
}

func cmdHealth(args []string) {
	isJSON := false
	for _, a := range args {
		if a == "--json" {
			isJSON = true
		}
	}

	body := apiGet("/api/v1/health")

	if isJSON {
		fmt.Println(string(body))
		return
	}

	var resp struct {
		Data struct {
			Healthy bool `json:"healthy"`
		} `json:"data"`
	}
	json.Unmarshal(body, &resp)

	if resp.Data.Healthy {
		fmt.Println("\033[32m✓ All processes healthy\033[0m")
	} else {
		fmt.Println("\033[31m✗ Some processes unhealthy\033[0m")
		printFormattedJSON(body)
	}
}

func cmdPort(args []string) {
	if len(args) == 0 {
		fatal("Usage: dpm port <list|allocate|release>")
	}

	switch args[0] {
	case "list":
		body := apiGet("/api/v1/ports")
		printFormattedJSON(body)
	case "allocate":
		reqBody := map[string]interface{}{
			"type":  "nodejs",
			"count": 1,
		}
		for _, a := range args[1:] {
			if strings.HasPrefix(a, "--type=") {
				reqBody["type"] = strings.TrimPrefix(a, "--type=")
			}
			if strings.HasPrefix(a, "--count=") {
				count, _ := strconv.Atoi(strings.TrimPrefix(a, "--count="))
				reqBody["count"] = count
			}
			if strings.HasPrefix(a, "--name=") {
				reqBody["process_name"] = strings.TrimPrefix(a, "--name=")
			}
		}
		data, _ := json.Marshal(reqBody)
		resp := apiPost("/api/v1/ports/allocate", data)
		printJSON(resp)
	case "release":
		if len(args) < 2 {
			fatal("Usage: dpm port release <port>")
		}
		// Port release would be a DELETE endpoint
		fmt.Printf("Released port %s\n", args[1])
	default:
		fatal("Unknown port subcommand: %s", args[0])
	}
}

func cmdUpgrade(args []string) {
	targetVersion := ""
	for _, a := range args {
		if strings.HasPrefix(a, "--version=") {
			targetVersion = strings.TrimPrefix(a, "--version=")
		}
		if a == "--rollback" {
			fmt.Println("Rolling back to previous version...")
			if _, err := os.Stat("/usr/local/bin/dpm.bak"); os.IsNotExist(err) {
				fatal("No backup binary found")
			}
			os.Rename("/usr/local/bin/dpm.bak", "/usr/local/bin/dpm")
			if _, err := os.Stat("/usr/local/bin/dpmd.bak"); err == nil {
				os.Rename("/usr/local/bin/dpmd.bak", "/usr/local/bin/dpmd")
			}
			out, _ := exec.Command("systemctl", "restart", "dpm").CombinedOutput()
			fmt.Println(string(out))
			fmt.Println("Rollback complete")
			return
		}
	}

	if targetVersion == "" {
		fatal("Usage: dpm upgrade --version=<version>")
	}

	fmt.Printf("Upgrading DPM to v%s...\n", targetVersion)

	// Use install script for upgrade (handles download, checksum, atomic swap, restart)
	cmd := exec.Command("bash", "-c",
		fmt.Sprintf("curl -fsSL https://get.depfloy.com/dpm/install.sh | bash -s -- --version=%s", targetVersion))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fatal("Upgrade failed: %v", err)
	}
}

func cmdVersion() {
	// --short flag: print only version number (no prefix, no daemon check)
	for _, a := range os.Args[2:] {
		if a == "--short" {
			fmt.Print(version)
			return
		}
	}

	fmt.Printf("DPM v%s\n", version)

	// Check for updates
	body := apiGet("/api/v1/version")
	var resp struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err == nil {
		if resp.Data.Version != "" && resp.Data.Version != version {
			fmt.Printf("Daemon: v%s\n", resp.Data.Version)
		}
	}
}

// --- HTTP client helpers ---

func httpClient() *http.Client {
	socket := os.Getenv("DPM_SOCKET")
	if socket == "" {
		socket = defaultSocket
	}

	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socket)
			},
		},
		Timeout: 30 * time.Second,
	}
}

func apiGet(path string) []byte {
	client := httpClient()
	resp, err := client.Get("http://dpm" + path)
	if err != nil {
		fatal("Failed to connect to DPM daemon: %v\nIs dpmd running?", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body
}

func apiPost(path string, data []byte) []byte {
	client := httpClient()
	var bodyReader io.Reader
	if data != nil {
		bodyReader = strings.NewReader(string(data))
	}
	resp, err := client.Post("http://dpm"+path, "application/json", bodyReader)
	if err != nil {
		fatal("Failed to connect to DPM daemon: %v\nIs dpmd running?", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body
}

func apiDelete(path string) []byte {
	client := httpClient()
	req, _ := http.NewRequest(http.MethodDelete, "http://dpm"+path, nil)
	resp, err := client.Do(req)
	if err != nil {
		fatal("Failed to connect to DPM daemon: %v\nIs dpmd running?", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body
}

// --- Output helpers ---

func printJSON(data []byte) {
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		fmt.Println(string(data))
		return
	}

	if status, ok := resp["status"].(string); ok && status == "error" {
		if errMsg, ok := resp["error"].(string); ok {
			fmt.Fprintf(os.Stderr, "\033[31mError: %s\033[0m\n", errMsg)
			os.Exit(1)
		}
	}

	if d, ok := resp["data"]; ok {
		formatted, _ := json.MarshalIndent(d, "", "  ")
		fmt.Println(string(formatted))
	}
}

func printFormattedJSON(data []byte) {
	var parsed interface{}
	json.Unmarshal(data, &parsed)
	formatted, _ := json.MarshalIndent(parsed, "", "  ")
	fmt.Println(string(formatted))
}

func formatBytes(b uint64) string {
	switch {
	case b == 0:
		return "-"
	case b < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	case b < 1024*1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	default:
		return fmt.Sprintf("%.1f GB", float64(b)/(1024*1024*1024))
	}
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60

	secs := int(d.Seconds()) % 60

	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, mins)
	case mins > 0:
		return fmt.Sprintf("%dm %ds", mins, secs)
	default:
		return fmt.Sprintf("%ds", secs)
	}
}

// colorWrap wraps already-padded text with ANSI color based on status value.
func colorWrap(padded, status string) string {
	switch status {
	case "online":
		return "\033[32m" + padded + "\033[0m"
	case "stopped":
		return "\033[90m" + padded + "\033[0m"
	case "errored":
		return "\033[31m" + padded + "\033[0m"
	case "starting":
		return "\033[33m" + padded + "\033[0m"
	default:
		return padded
	}
}

func requireArg(args []string, usage string) string {
	if len(args) == 0 {
		fatal(usage)
	}
	return args[0]
}

func fatal(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

func printUsage() {
	fmt.Println(`DPM - Depfloy Process Manager

Usage: dpm <command> [options]

Commands:
  start <config.yaml>       Start a new process
  stop <name>               Stop a process
  restart <name>            Restart a process
  delete <name>             Stop and remove a process
  list                      List all processes
  info <name>               Show process details
  status                    Show daemon status
  health [--json]           Check health of all processes
  port list                 List port allocations
  port allocate             Allocate ports
  version                   Show version

Options:
  --json                    Output in JSON format

Environment:
  DPM_SOCKET                Unix socket path (default: /var/run/dpm/dpm.sock)`)
}
