package nginx

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/depfloy/dpm/internal/process"
)

//go:embed templates/*.tmpl
var templateFS embed.FS

// ApplyRequest defines what Depfloy sends to configure nginx for a process.
type ApplyRequest struct {
	ProcessName   string          `json:"process_name"`
	ProjectID     int             `json:"project_id"`
	Type          string          `json:"type"` // nodejs, php
	Domains       []string        `json:"domains"`
	PrimaryDomain string          `json:"primary_domain"`
	WWWRedirect   int             `json:"www_redirect"` // 0, 1, 2
	SSL           *SSLConfig      `json:"ssl,omitempty"`
	RootDirectory string          `json:"root_directory"`
	PHPVersion    string          `json:"php_version,omitempty"`
	ErrorLog      string          `json:"error_log,omitempty"`
	CustomSnippets *CustomSnippets `json:"custom_snippets,omitempty"`
}

// SSLConfig holds SSL certificate paths.
type SSLConfig struct {
	Enabled  bool   `json:"enabled"`
	CertPath string `json:"cert_path"`
	KeyPath  string `json:"key_path"`
}

// CustomSnippets holds user-defined nginx config fragments to inject.
type CustomSnippets struct {
	Server         string `json:"server"`          // Injected into server {} block
	Location       string `json:"location"`        // Injected into location / {} block
	ExtraLocations string `json:"extra_locations"` // Appended before closing server }
}

// ApplyResult is returned after applying nginx config.
type ApplyResult struct {
	Status           string `json:"status"`
	ConfigPath       string `json:"config_path"`
	NginxTest        string `json:"nginx_test"`
	Reloaded         bool   `json:"reloaded"`
	WorkersInUpstream int   `json:"workers_in_upstream"`
	Error            string `json:"error,omitempty"`
}

// Manager handles nginx configuration lifecycle.
type Manager struct {
	configDir     string // /etc/nginx
	reloadCommand string
	pm            *process.Manager
}

// NewManager creates a new nginx manager.
func NewManager(configDir, reloadCommand string, pm *process.Manager) *Manager {
	return &Manager{
		configDir:     configDir,
		reloadCommand: reloadCommand,
		pm:            pm,
	}
}

// Apply generates nginx config, writes it, validates, and reloads.
func (m *Manager) Apply(req *ApplyRequest) *ApplyResult {
	result := &ApplyResult{}

	// 1. Get worker info for upstream
	var workers []process.Info
	if req.Type == "nodejs" && req.ProcessName != "" {
		infos, err := m.pm.GetInfo(req.ProcessName)
		if err == nil {
			workers = infos
		}
	}

	// 2. Generate config
	config := m.generate(req, workers)
	result.WorkersInUpstream = len(workers)

	// 3. Write config
	configPath := filepath.Join(m.configDir, "sites-available", req.PrimaryDomain)
	result.ConfigPath = configPath

	// Backup existing config
	backupPath := configPath + ".bak"
	if _, err := os.Stat(configPath); err == nil {
		copyFile(configPath, backupPath)
	}

	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("write config failed: %v", err)
		return result
	}

	// 4. Ensure symlink
	symlinkPath := filepath.Join(m.configDir, "sites-enabled", req.PrimaryDomain)
	os.Remove(symlinkPath)
	os.Symlink(configPath, symlinkPath)

	// 5. Validate
	testOutput, err := exec.Command("nginx", "-t").CombinedOutput()
	result.NginxTest = strings.TrimSpace(string(testOutput))

	if err != nil {
		// Rollback
		if _, bakErr := os.Stat(backupPath); bakErr == nil {
			os.Rename(backupPath, configPath)
		} else {
			os.Remove(configPath)
			os.Remove(symlinkPath)
		}
		result.Status = "error"
		result.Error = fmt.Sprintf("nginx config test failed: %s", result.NginxTest)
		return result
	}

	// 6. Reload
	reloadOutput, err := exec.Command("sh", "-c", m.reloadCommand).CombinedOutput()
	if err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("nginx reload failed: %s", string(reloadOutput))
		return result
	}

	// Cleanup backup
	os.Remove(backupPath)

	result.Status = "success"
	result.Reloaded = true
	return result
}

// Remove deletes nginx config for a domain and reloads.
func (m *Manager) Remove(domain string) *ApplyResult {
	result := &ApplyResult{}

	configPath := filepath.Join(m.configDir, "sites-available", domain)
	symlinkPath := filepath.Join(m.configDir, "sites-enabled", domain)

	os.Remove(symlinkPath)
	os.Remove(configPath)

	reloadOutput, err := exec.Command("sh", "-c", m.reloadCommand).CombinedOutput()
	if err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("nginx reload failed: %s", string(reloadOutput))
		return result
	}

	result.Status = "success"
	result.Reloaded = true
	return result
}

// Show returns the current nginx config for a domain.
func (m *Manager) Show(domain string) (string, error) {
	configPath := filepath.Join(m.configDir, "sites-available", domain)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("config not found: %s", domain)
	}
	return string(data), nil
}

// Test runs nginx -t and returns the result.
func (m *Manager) Test() (string, error) {
	output, err := exec.Command("nginx", "-t").CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

// Status returns a list of configured sites.
func (m *Manager) Status() ([]string, error) {
	enabledDir := filepath.Join(m.configDir, "sites-enabled")
	entries, err := os.ReadDir(enabledDir)
	if err != nil {
		return nil, err
	}

	var sites []string
	for _, e := range entries {
		sites = append(sites, e.Name())
	}
	return sites, nil
}

// generate builds the full nginx config from template + request data.
func (m *Manager) generate(req *ApplyRequest, workers []process.Info) string {
	// Select template
	tmplName := req.Type + "-http"
	if req.SSL != nil && req.SSL.Enabled {
		tmplName = req.Type + "-ssl"
	}
	tmpl := loadTemplate(tmplName)

	// Build upstream (nodejs only)
	upstreamBlock := ""
	if req.Type == "nodejs" {
		upstreamBlock = buildUpstream(req.ProjectID, workers)
	}

	// Build domains (handle www redirect)
	domains, redirectBlock := buildDomains(req)

	// Replace placeholders
	config := tmpl
	config = strings.ReplaceAll(config, "{{UPSTREAM_BLOCK}}", upstreamBlock)
	config = strings.ReplaceAll(config, "{{DOMAINS}}", strings.Join(domains, " "))
	config = strings.ReplaceAll(config, "{{ROOT_DIRECTORY}}", req.RootDirectory)
	config = strings.ReplaceAll(config, "{{PROJECT_ID}}", strconv.Itoa(req.ProjectID))
	config = strings.ReplaceAll(config, "{{PHP_VERSION}}", req.PHPVersion)

	if req.SSL != nil && req.SSL.Enabled {
		config = strings.ReplaceAll(config, "{{SSL_CERT_PATH}}", req.SSL.CertPath)
		config = strings.ReplaceAll(config, "{{SSL_KEY_PATH}}", req.SSL.KeyPath)
	}

	// Inject custom snippets
	config = injectSnippets(config, req.CustomSnippets)

	// Append redirect block if www redirect is configured
	if redirectBlock != "" {
		config += "\n" + redirectBlock
	}

	return config
}

// buildUpstream creates the nginx upstream block from worker info.
func buildUpstream(projectID int, workers []process.Info) string {
	b := fmt.Sprintf("upstream upstream_%d {\n", projectID)
	b += "    least_conn;\n"

	activeWorkers := 0
	for _, w := range workers {
		if w.Port > 0 {
			b += fmt.Sprintf("    server 127.0.0.1:%d max_fails=2 fail_timeout=5s;\n", w.Port)
			activeWorkers++
		}
	}

	if activeWorkers == 0 {
		// Fallback: no workers known yet
		b += "    server 127.0.0.1:3000 max_fails=2 fail_timeout=5s;\n"
		activeWorkers = 1
	}

	keepalive := activeWorkers * 16
	if keepalive < 32 {
		keepalive = 32
	}
	b += fmt.Sprintf("    keepalive %d;\n", keepalive)
	b += "}"
	return b
}

// buildDomains processes domains based on www redirect mode.
// Returns the filtered domain list and an optional redirect server block.
func buildDomains(req *ApplyRequest) ([]string, string) {
	domains := make([]string, len(req.Domains))
	copy(domains, req.Domains)

	primaryNoWWW := strings.TrimPrefix(req.PrimaryDomain, "www.")
	wwwDomain := "www." + primaryNoWWW

	scheme := "http"
	if req.SSL != nil && req.SSL.Enabled {
		scheme = "https"
	}

	switch req.WWWRedirect {
	case 1: // www → non-www
		// Remove www from main block
		filtered := make([]string, 0, len(domains))
		for _, d := range domains {
			if d != wwwDomain {
				filtered = append(filtered, d)
			}
		}
		domains = filtered

		// Build redirect block
		redirect := fmt.Sprintf("server {\n    listen 80;\n    listen [::]:80;\n    server_name %s;\n    server_tokens off;\n\n    return 308 %s://%s$request_uri;\n}", wwwDomain, scheme, primaryNoWWW)
		if req.SSL != nil && req.SSL.Enabled {
			redirect += fmt.Sprintf("\n\nserver {\n    listen 443 ssl http2;\n    listen [::]:443 ssl http2;\n    server_name %s;\n    server_tokens off;\n\n    ssl_certificate %s;\n    ssl_certificate_key %s;\n\n    return 308 https://%s$request_uri;\n}", wwwDomain, req.SSL.CertPath, req.SSL.KeyPath, primaryNoWWW)
		}
		return domains, redirect

	case 2: // non-www → www
		// Remove non-www, ensure www is present
		filtered := make([]string, 0, len(domains))
		for _, d := range domains {
			if d != primaryNoWWW {
				filtered = append(filtered, d)
			}
		}
		if !contains(filtered, wwwDomain) {
			filtered = append(filtered, wwwDomain)
		}
		domains = filtered

		redirect := fmt.Sprintf("server {\n    listen 80;\n    listen [::]:80;\n    server_name %s;\n    server_tokens off;\n\n    return 308 %s://%s$request_uri;\n}", primaryNoWWW, scheme, wwwDomain)
		if req.SSL != nil && req.SSL.Enabled {
			redirect += fmt.Sprintf("\n\nserver {\n    listen 443 ssl http2;\n    listen [::]:443 ssl http2;\n    server_name %s;\n    server_tokens off;\n\n    ssl_certificate %s;\n    ssl_certificate_key %s;\n\n    return 308 https://%s$request_uri;\n}", primaryNoWWW, req.SSL.CertPath, req.SSL.KeyPath, wwwDomain)
		}
		return domains, redirect
	}

	return domains, ""
}

// injectSnippets replaces snippet placeholders with user content.
func injectSnippets(config string, snippets *CustomSnippets) string {
	if snippets == nil {
		config = strings.ReplaceAll(config, "{{CUSTOM_SERVER_SNIPPET}}", "")
		config = strings.ReplaceAll(config, "{{CUSTOM_LOCATION_SNIPPET}}", "")
		config = strings.ReplaceAll(config, "{{CUSTOM_EXTRA_LOCATIONS}}", "")
		return config
	}

	config = strings.ReplaceAll(config, "{{CUSTOM_SERVER_SNIPPET}}", indent(snippets.Server, "    "))
	config = strings.ReplaceAll(config, "{{CUSTOM_LOCATION_SNIPPET}}", indent(snippets.Location, "        "))
	config = strings.ReplaceAll(config, "{{CUSTOM_EXTRA_LOCATIONS}}", indent(snippets.ExtraLocations, "    "))
	return config
}

// loadTemplate reads an embedded template file.
func loadTemplate(name string) string {
	filename := fmt.Sprintf("templates/%s.conf.tmpl", name)
	data, err := templateFS.ReadFile(filename)
	if err != nil {
		return ""
	}
	return string(data)
}

// indent adds a prefix to each non-empty line.
func indent(s, prefix string) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}
	return strings.Join(lines, "\n")
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}

// ParseApplyRequest parses JSON into an ApplyRequest.
func ParseApplyRequest(data []byte) (*ApplyRequest, error) {
	var req ApplyRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}
