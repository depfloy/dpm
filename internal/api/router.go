package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/depfloy/dpm/internal/health"
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
	store *state.Store,
	cfg *config.DaemonConfig,
	logger *slog.Logger,
) http.Handler {
	r := &Router{
		pm:     pm,
		ports:  ports,
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
	mux.HandleFunc("/api/v1/ports/allocate", r.handlePortAllocate)

	// System endpoints
	mux.HandleFunc("/api/v1/status", r.handleStatus)
	mux.HandleFunc("/api/v1/health", r.handleHealth)
	mux.HandleFunc("/api/v1/version", r.handleVersion)

	return mux
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

	// Allocate ports - use ResolveWorkerCount for cluster-aware count
	workerCount := cfg.ResolveWorkerCount()
	var ports []int
	if cfg.Port == "auto" {
		allocated, err := r.ports.Allocate(cfg.Name, cfg.Type, workerCount)
		if err != nil {
			r.errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("port allocation failed: %v", err))
			return
		}
		ports = allocated
	} else if cfg.Port != "" {
		p, err := strconv.Atoi(cfg.Port)
		if err != nil {
			r.errorResponse(w, http.StatusBadRequest, "port must be 'auto' or a number")
			return
		}
		// First port is the specified one
		ports = []int{p}
		// Allocate remaining ports for additional workers
		if workerCount > 1 {
			additional, err := r.ports.Allocate(cfg.Name, cfg.Type, workerCount-1)
			if err != nil {
				r.errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("additional port allocation failed: %v", err))
				return
			}
			ports = append(ports, additional...)
		}
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

	ports, _ := r.ports.GetByProcess(name)
	if err := r.pm.Start(cfg, ports); err != nil {
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

func (r *Router) handlePortAllocate(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		r.methodNotAllowed(w)
		return
	}

	var body struct {
		ProcessName string `json:"process_name"`
		Type        string `json:"type"`
		Count       int    `json:"count"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if body.Count <= 0 {
		body.Count = 1
	}

	ports, err := r.ports.Allocate(body.ProcessName, body.Type, body.Count)
	if err != nil {
		r.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.successResponse(w, map[string]interface{}{"ports": ports})
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
