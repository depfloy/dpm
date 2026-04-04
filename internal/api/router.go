package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/depfloy/dpm/internal/health"
	dpmlog "github.com/depfloy/dpm/internal/log"
	"github.com/depfloy/dpm/internal/nginx"
	"github.com/depfloy/dpm/internal/port"
	"github.com/depfloy/dpm/internal/process"
	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

// Response is the standard API response format.
type Response struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
	Meta   Meta        `json:"meta"`
}

// Meta contains response metadata.
type Meta struct {
	Version string `json:"version"`
}

// Router sets up HTTP routes for the DPM API.
type Router struct {
	pm      *process.Manager
	ports   *port.Manager
	health  *health.Checker
	nginx   *nginx.Manager
	store   *state.Store
	config  *config.DaemonConfig
	logger  *slog.Logger
	version string
}

// NewRouter creates a new API router with all handlers registered.
func NewRouter(
	pm *process.Manager,
	ports *port.Manager,
	hc *health.Checker,
	nginxMgr *nginx.Manager,
	store *state.Store,
	cfg *config.DaemonConfig,
	logger *slog.Logger,
) http.Handler {
	r := &Router{
		pm:     pm,
		ports:  ports,
		nginx:  nginxMgr,
		health: hc,
		store:  store,
		config: cfg,
		logger: logger,
	}

	mux := http.NewServeMux()

	// Process endpoints
	mux.HandleFunc("/api/v1/processes", r.handleProcesses)
	mux.HandleFunc("/api/v1/processes/", r.handleProcess)

	// Port endpoints
	mux.HandleFunc("/api/v1/ports", r.handlePorts)

	// Nginx endpoints
	mux.HandleFunc("/api/v1/nginx/apply", r.handleNginxApply)
	mux.HandleFunc("/api/v1/nginx/remove/", r.handleNginxRemove)
	mux.HandleFunc("/api/v1/nginx/show/", r.handleNginxShow)
	mux.HandleFunc("/api/v1/nginx/test", r.handleNginxTest)
	mux.HandleFunc("/api/v1/nginx/status", r.handleNginxStatus)

	// Log endpoints
	mux.HandleFunc("/api/v1/logs/", r.handleLogs)

	// System endpoints
	mux.HandleFunc("/api/v1/status", r.handleStatus)
	mux.HandleFunc("/api/v1/health", r.handleHealth)
	mux.HandleFunc("/api/v1/version", r.handleVersion)
	mux.HandleFunc("/api/v1/system/reload", r.handleReload)
	mux.HandleFunc("/api/v1/system/doctor", r.handleDoctor)

	// Wrap all handlers with panic recovery
	return panicRecovery(mux, logger)
}

// panicRecovery wraps an http.Handler to recover from panics instead of crashing the daemon.
func panicRecovery(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("API handler panic recovered", "error", fmt.Sprintf("%v", err), "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{
					"status": "error",
					"error":  fmt.Sprintf("internal error: %v", err),
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// --- Process Handlers ---

func (r *Router) handleProcesses(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		r.listProcesses(w, req)
	case http.MethodPost:
		r.createProcess(w, req)
	default:
		r.methodNotAllowed(w)
	}
}

func (r *Router) handleProcess(w http.ResponseWriter, req *http.Request) {
	// Extract process name and optional action from path
	// /api/v1/processes/{name}
	// /api/v1/processes/{name}/start
	// /api/v1/processes/{name}/stop
	// /api/v1/processes/{name}/restart
	path := strings.TrimPrefix(req.URL.Path, "/api/v1/processes/")
	parts := strings.SplitN(path, "/", 2)
	name := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	if name == "" {
		r.errorResponse(w, http.StatusBadRequest, "process name required")
		return
	}

	switch {
	case action == "" && req.Method == http.MethodGet:
		r.getProcess(w, name)
	case action == "" && req.Method == http.MethodDelete:
		r.deleteProcess(w, name)
	case action == "start" && req.Method == http.MethodPost:
		r.startProcess(w, name)
	case action == "stop" && req.Method == http.MethodPost:
		r.stopProcess(w, name)
	case action == "restart" && req.Method == http.MethodPost:
		r.restartProcess(w, name)
	case action == "deploy" && req.Method == http.MethodPost:
		r.deployProcess(w, req)
	case action == "drain" && req.Method == http.MethodPost:
		r.drainProcess(w, name)
	default:
		r.methodNotAllowed(w)
	}
}

func (r *Router) listProcesses(w http.ResponseWriter, _ *http.Request) {
	infos := r.pm.List()
	r.successResponse(w, infos)
}

func (r *Router) createProcess(w http.ResponseWriter, req *http.Request) {
	var cfg config.ProcessConfig
	if err := json.NewDecoder(req.Body).Decode(&cfg); err != nil {
		r.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid config: %v", err))
		return
	}

	if cfg.Name == "" || cfg.Command == "" {
		r.errorResponse(w, http.StatusBadRequest, "name and command are required")
		return
	}

	// Require explicit ports array from Depfloy - no auto-allocation
	if len(cfg.Ports) == 0 {
		r.errorResponse(w, http.StatusBadRequest, "ports array is required")
		return
	}

	// Stop existing process BEFORE port check - the old process is likely
	// holding the same ports we want to reuse
	if err := r.pm.Stop(cfg.Name); err != nil {
		r.logger.Debug("no existing process to stop", "name", cfg.Name)
	}

	// Release old port allocations from state store
	if err := r.ports.Release(cfg.Name); err != nil {
		r.logger.Warn("failed to release old ports", "name", cfg.Name, "error", err)
	}

	// Now check port availability - ports should be free after stopping old process
	var ports []int
	for _, p := range cfg.Ports {
		if !r.ports.IsPortFree(p) {
			r.errorResponse(w, http.StatusConflict, fmt.Sprintf("port %d is busy, cannot start process", p))
			return
		}
		ports = append(ports, p)
	}

	// Start process
	if err := r.pm.Start(&cfg, ports); err != nil {
		r.errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("start failed: %v", err))
		return
	}

	// Start health monitoring if configured
	if cfg.HealthCheck != nil && len(ports) > 0 {
		r.health.StartMonitoring(cfg.Name, ports[0], cfg.HealthCheck)
	}

	r.logger.Info("process created",
		"name", cfg.Name,
		"type", cfg.Type,
		"ports", ports,
	)

	r.successResponse(w, map[string]interface{}{
		"name":  cfg.Name,
		"ports": ports,
	})
}

func (r *Router) getProcess(w http.ResponseWriter, name string) {
	infos, err := r.pm.GetInfo(name)
	if err != nil {
		r.errorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	healthStatus := r.health.GetStatus(name)

	r.successResponse(w, map[string]interface{}{
		"instances": infos,
		"health":    healthStatus,
	})
}

func (r *Router) deleteProcess(w http.ResponseWriter, name string) {
	r.health.StopMonitoring(name)

	if err := r.pm.Delete(name); err != nil {
		r.errorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	if err := r.ports.Release(name); err != nil {
		r.logger.Warn("failed to release ports", "name", name, "error", err)
	}

	r.logger.Info("process deleted", "name", name)
	r.successResponse(w, map[string]string{"deleted": name})
}

func (r *Router) startProcess(w http.ResponseWriter, name string) {
	cfg := r.pm.GetConfig(name)
	if cfg == nil {
		r.errorResponse(w, http.StatusNotFound, "process not found")
		return
	}

	if err := r.pm.Start(cfg, cfg.Ports); err != nil {
		r.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.successResponse(w, map[string]string{"started": name})
}

func (r *Router) stopProcess(w http.ResponseWriter, name string) {
	if err := r.pm.Stop(name); err != nil {
		r.errorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	r.logger.Info("process stopped", "name", name)
	r.successResponse(w, map[string]string{"stopped": name})
}

func (r *Router) deployProcess(w http.ResponseWriter, req *http.Request) {
	var cfg config.ProcessConfig
	if err := json.NewDecoder(req.Body).Decode(&cfg); err != nil {
		r.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid config: %v", err))
		return
	}

	if cfg.Name == "" || cfg.Command == "" {
		r.errorResponse(w, http.StatusBadRequest, "name and command are required")
		return
	}

	// Require explicit ports array from Depfloy - no auto-allocation
	if len(cfg.Ports) == 0 {
		r.errorResponse(w, http.StatusBadRequest, "ports array is required")
		return
	}

	// Check new port availability (old ports are still in use by running process)
	var newPorts []int
	for _, p := range cfg.Ports {
		if !r.ports.IsPortFree(p) {
			r.errorResponse(w, http.StatusConflict, fmt.Sprintf("port %d is busy, cannot deploy", p))
			return
		}
		newPorts = append(newPorts, p)
	}

	// Blue-green deploy: start new workers while old ones keep serving
	result, err := r.pm.Deploy(&cfg, newPorts)
	if err != nil {
		r.errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("deploy failed: %v", err))
		return
	}

	// Start health monitoring if configured
	if cfg.HealthCheck != nil && len(newPorts) > 0 {
		r.health.StartMonitoring(cfg.Name, newPorts[0], cfg.HealthCheck)
	}

	r.logger.Info("blue-green deploy completed",
		"name", cfg.Name,
		"new_ports", newPorts,
		"old_ports", result.OldPorts,
		"workers", result.Workers,
	)

	r.successResponse(w, result)
}

func (r *Router) drainProcess(w http.ResponseWriter, name string) {
	if err := r.pm.Drain(name); err != nil {
		r.errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("drain failed: %v", err))
		return
	}

	r.logger.Info("old workers drained", "name", name)
	r.successResponse(w, map[string]string{"drained": name})
}

func (r *Router) restartProcess(w http.ResponseWriter, name string) {
	if err := r.pm.Restart(name); err != nil {
		r.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.logger.Info("process restarted", "name", name)
	r.successResponse(w, map[string]string{"restarted": name})
}

// --- Port Handlers ---

func (r *Router) handlePorts(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		r.methodNotAllowed(w)
		return
	}

	allocations, err := r.ports.List()
	if err != nil {
		r.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.successResponse(w, allocations)
}

// --- Log Handlers ---

func (r *Router) handleLogs(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		r.methodNotAllowed(w)
		return
	}

	name := strings.TrimPrefix(req.URL.Path, "/api/v1/logs/")
	if name == "" {
		r.errorResponse(w, http.StatusBadRequest, "process name required")
		return
	}

	lines := 100
	if v := req.URL.Query().Get("lines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			lines = n
		}
	}

	level := req.URL.Query().Get("level")
	follow := req.URL.Query().Get("follow") == "true"
	format := req.URL.Query().Get("format") // "json" or empty for plain text

	// Read log files
	logDir := filepath.Join(r.config.Logging.Dir, "apps", name)
	var allLines []string

	// Read main log + instance logs
	logFiles := []string{"current.log", "instance-1.log", "instance-2.log", "instance-3.log"}
	if level == "error" {
		logFiles = []string{"error.log", "instance-1.error.log", "instance-2.error.log", "instance-3.error.log"}
	}

	for _, f := range logFiles {
		path := filepath.Join(logDir, f)
		fileLines := readLastLines(path, lines)
		allLines = append(allLines, fileLines...)
	}

	// Also include error logs if not filtering by level
	if level == "" {
		errorFiles := []string{"error.log", "instance-1.error.log", "instance-2.error.log", "instance-3.error.log"}
		for _, f := range errorFiles {
			path := filepath.Join(logDir, f)
			fileLines := readLastLines(path, lines)
			allLines = append(allLines, fileLines...)
		}
	}

	// Sort all lines by timestamp descending (newest first).
	// DPM prefixes each line with an RFC3339 timestamp like "2026-03-23T10:05:42Z ...".
	sort.SliceStable(allLines, func(i, j int) bool {
		ti := parseLineTimestamp(allLines[i])
		tj := parseLineTimestamp(allLines[j])
		return ti.After(tj)
	})

	// Limit total
	if len(allLines) > lines {
		allLines = allLines[:lines]
	}

	if format == "json" {
		// Parse lines into structured entries, merging stack trace continuations
		engine := dpmlog.NewEngine(r.config.Logging.Dir)
		var entries []dpmlog.Entry
		for _, line := range allLines {
			entry := engine.ParseLine(line, name)
			msg := strings.TrimSpace(entry.Message)

			// Merge into previous entry if this is a continuation line
			// (stack trace "at ...", braces, error properties)
			if len(entries) > 0 && isContinuation(msg) {
				prev := &entries[len(entries)-1]
				prev.Message += "\n" + msg
			} else {
				entries = append(entries, entry)
			}
		}
		r.successResponse(w, entries)
	} else {
		w.Header().Set("Content-Type", "text/plain")
		for _, line := range allLines {
			fmt.Fprintln(w, line)
		}

		// Follow mode: stream new lines
		if follow {
			flusher, ok := w.(http.Flusher)
			if ok {
				flusher.Flush()
			}

			// Track file sizes
			logDir := filepath.Join(r.config.Logging.Dir, "apps", name)
			positions := make(map[string]int64)
			allFiles := []string{"current.log", "instance-1.log", "instance-2.log", "instance-3.log",
				"error.log", "instance-1.error.log", "instance-2.error.log", "instance-3.error.log"}
			if level == "error" {
				allFiles = []string{"error.log", "instance-1.error.log", "instance-2.error.log", "instance-3.error.log"}
			}
			for _, f := range allFiles {
				p := filepath.Join(logDir, f)
				if info, err := os.Stat(p); err == nil {
					positions[p] = info.Size()
				}
			}

			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-req.Context().Done():
					return
				case <-ticker.C:
					for _, f := range allFiles {
						p := filepath.Join(logDir, f)
						info, err := os.Stat(p)
						if err != nil {
							continue
						}
						pos := positions[p]
						if info.Size() <= pos {
							continue
						}
						file, err := os.Open(p)
						if err != nil {
							continue
						}
						file.Seek(pos, 0)
						scanner := bufio.NewScanner(file)
						for scanner.Scan() {
							line := scanner.Text()
							if line != "" {
								fmt.Fprintln(w, line)
							}
						}
						positions[p] = info.Size()
						file.Close()
						if ok {
							flusher.Flush()
						}
					}
				}
			}
		}
	}
}

func readLastLines(path string, n int) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256*1024), 256*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
	}

	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

// isContinuation detects stack trace and multi-line error continuation lines.
func isContinuation(msg string) bool {
	if msg == "" {
		return false
	}
	if strings.HasPrefix(msg, "at ") {
		return true
	}
	trimmed := strings.TrimSpace(msg)
	if trimmed == "}" || trimmed == "{" || trimmed == "})" || trimmed == "});" {
		return true
	}
	if strings.HasPrefix(trimmed, "code:") || strings.HasPrefix(trimmed, "errno:") ||
		strings.HasPrefix(trimmed, "syscall:") || strings.HasPrefix(trimmed, "address:") ||
		strings.HasPrefix(trimmed, "port:") {
		return true
	}
	return false
}

// parseLineTimestamp extracts the timestamp from a DPM log line prefix.
// Expected format: "2026-03-23T10:05:42Z message..." or RFC3339 with offset.
// Returns time.Time zero value if parsing fails, which sorts these lines last.
func parseLineTimestamp(line string) time.Time {
	if len(line) < 20 {
		return time.Time{}
	}

	// Try "2006-01-02T15:04:05Z" (20 chars)
	if t, err := time.Parse("2006-01-02T15:04:05Z", line[:20]); err == nil {
		return t
	}

	// Try RFC3339 with timezone offset (25 chars)
	if len(line) >= 25 {
		if t, err := time.Parse(time.RFC3339, line[:25]); err == nil {
			return t
		}
	}

	return time.Time{}
}

// --- Nginx Handlers ---

func (r *Router) handleNginxApply(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		r.methodNotAllowed(w)
		return
	}

	var applyReq nginx.ApplyRequest
	if err := json.NewDecoder(req.Body).Decode(&applyReq); err != nil {
		r.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	result := r.nginx.Apply(&applyReq)
	if result.Status == "error" {
		r.errorResponse(w, http.StatusInternalServerError, result.Error)
		return
	}

	r.logger.Info("nginx config applied", "domain", applyReq.PrimaryDomain, "workers", result.WorkersInUpstream)
	r.successResponse(w, result)
}

func (r *Router) handleNginxRemove(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodDelete && req.Method != http.MethodPost {
		r.methodNotAllowed(w)
		return
	}

	domain := strings.TrimPrefix(req.URL.Path, "/api/v1/nginx/remove/")
	if domain == "" {
		r.errorResponse(w, http.StatusBadRequest, "domain required")
		return
	}

	result := r.nginx.Remove(domain)
	if result.Status == "error" {
		r.errorResponse(w, http.StatusInternalServerError, result.Error)
		return
	}

	r.logger.Info("nginx config removed", "domain", domain)
	r.successResponse(w, result)
}

func (r *Router) handleNginxShow(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		r.methodNotAllowed(w)
		return
	}

	domain := strings.TrimPrefix(req.URL.Path, "/api/v1/nginx/show/")
	config, err := r.nginx.Show(domain)
	if err != nil {
		r.errorResponse(w, http.StatusNotFound, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, config)
}

func (r *Router) handleNginxTest(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodPost {
		r.methodNotAllowed(w)
		return
	}

	output, err := r.nginx.Test()
	status := "ok"
	if err != nil {
		status = "failed"
	}

	r.successResponse(w, map[string]string{"status": status, "output": output})
}

func (r *Router) handleNginxStatus(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		r.methodNotAllowed(w)
		return
	}

	sites, err := r.nginx.Status()
	if err != nil {
		r.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.successResponse(w, map[string]interface{}{"sites": sites, "count": len(sites)})
}

// --- System Handlers ---

func (r *Router) handleStatus(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		r.methodNotAllowed(w)
		return
	}

	processes := r.pm.List()
	ports, _ := r.ports.List()

	online := 0
	for _, p := range processes {
		if p.Status == "online" {
			online++
		}
	}

	r.successResponse(w, map[string]interface{}{
		"processes_total":  len(processes),
		"processes_online": online,
		"ports_allocated":  len(ports),
	})
}

func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		r.methodNotAllowed(w)
		return
	}

	processes := r.pm.List()
	healthStatuses := r.health.GetAllStatuses()

	allHealthy := true
	for _, p := range processes {
		if p.Status != "online" && p.Status != "stopped" {
			allHealthy = false
			break
		}
	}
	for _, h := range healthStatuses {
		if !h.Healthy {
			allHealthy = false
			break
		}
	}

	r.successResponse(w, map[string]interface{}{
		"healthy":   allHealthy,
		"processes": processes,
		"checks":    healthStatuses,
	})
}

func (r *Router) handleVersion(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		r.methodNotAllowed(w)
		return
	}

	r.successResponse(w, map[string]string{
		"version": "dev",
	})
}

func (r *Router) handleReload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		r.methodNotAllowed(w)
		return
	}

	r.logger.Info("reload-all requested")

	restarted, failed, err := r.pm.ReloadAll()
	if err != nil {
		r.errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("reload failed: %v", err))
		return
	}

	r.logger.Info("reload-all completed", "restarted", restarted, "failed", failed)

	r.successResponse(w, map[string]interface{}{
		"restarted": restarted,
		"failed":    failed,
	})
}

func (r *Router) handleDoctor(w http.ResponseWriter, req *http.Request) {
	fix := req.Method == http.MethodPost || req.URL.Query().Get("fix") == "true"

	r.logger.Info("doctor check requested", "fix", fix)

	report := r.pm.Doctor(fix)

	r.logger.Info("doctor check completed",
		"zombies", len(report.Zombies),
		"orphans", len(report.Orphans),
		"zombies_fixed", report.ZombiesFixed,
		"orphans_fixed", report.OrphansFixed,
	)

	r.successResponse(w, report)
}

// --- Response helpers ---

func (r *Router) successResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{
		Status: "success",
		Data:   data,
		Meta:   Meta{Version: "dev"},
	})
}

func (r *Router) errorResponse(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(Response{
		Status: "error",
		Error:  message,
		Meta:   Meta{Version: "dev"},
	})
}

func (r *Router) methodNotAllowed(w http.ResponseWriter) {
	r.errorResponse(w, http.StatusMethodNotAllowed, "method not allowed")
}
