package upgrade

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

const baseURL = "https://get.depfloy.com/dpm"

// ChannelInfo represents the release channel metadata.
type ChannelInfo struct {
	Version    string            `json:"version"`
	ReleasedAt time.Time         `json:"released_at"`
	Checksums  map[string]string `json:"checksums"`
	Notes      string            `json:"release_notes"`
	Severity   string            `json:"severity"`
	Changelog  []string          `json:"changelog"`
}

// Result represents the outcome of an upgrade.
type Result struct {
	Status             string `json:"status"` // success, failed, up_to_date
	PreviousVersion    string `json:"previous_version,omitempty"`
	NewVersion         string `json:"new_version,omitempty"`
	UpgradeTimeMs      int64  `json:"upgrade_time_ms,omitempty"`
	ProcessesReattached int   `json:"processes_reattached,omitempty"`
	ProcessesHealthy   int    `json:"processes_healthy,omitempty"`
	RollbackAvailable  bool   `json:"rollback_available"`
	Error              string `json:"error,omitempty"`
}

// CheckUpdate checks if a newer version is available.
func CheckUpdate(currentVersion, channel string) (*ChannelInfo, error) {
	if channel == "" {
		channel = "stable"
	}

	url := fmt.Sprintf("%s/channels/%s.json", baseURL, channel)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch channel info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("channel info not found (HTTP %d)", resp.StatusCode)
	}

	var info ChannelInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("parse channel info: %w", err)
	}

	return &info, nil
}

// Perform executes the DPM self-upgrade process.
func Perform(currentVersion, targetVersion, channel string, force bool) *Result {
	start := time.Now()
	result := &Result{
		PreviousVersion: currentVersion,
		NewVersion:      targetVersion,
	}

	// Check if already up to date
	if currentVersion == targetVersion && !force {
		result.Status = "up_to_date"
		return result
	}

	arch := runtime.GOARCH
	cliBinaryName := fmt.Sprintf("dpm-linux-%s", arch)
	daemonBinaryName := fmt.Sprintf("dpmd-linux-%s", arch)

	// 1. Download new binaries
	cliURL := fmt.Sprintf("%s/v%s/%s", baseURL, targetVersion, cliBinaryName)
	daemonURL := fmt.Sprintf("%s/v%s/%s", baseURL, targetVersion, daemonBinaryName)
	cliTmpPath := "/usr/local/bin/dpm.new"
	daemonTmpPath := "/usr/local/bin/dpmd.new"

	if err := downloadFile(cliTmpPath, cliURL); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("CLI download failed: %v", err)
		return result
	}

	if err := downloadFile(daemonTmpPath, daemonURL); err != nil {
		os.Remove(cliTmpPath)
		result.Status = "failed"
		result.Error = fmt.Sprintf("daemon download failed: %v", err)
		return result
	}

	// 2. Download and verify checksums
	checksumURL := fmt.Sprintf("%s/v%s/checksums.txt", baseURL, targetVersion)
	if err := verifyChecksum(cliTmpPath, checksumURL, cliBinaryName); err != nil {
		os.Remove(cliTmpPath)
		os.Remove(daemonTmpPath)
		result.Status = "failed"
		result.Error = fmt.Sprintf("CLI checksum verification failed: %v", err)
		return result
	}

	if err := verifyChecksum(daemonTmpPath, checksumURL, daemonBinaryName); err != nil {
		os.Remove(cliTmpPath)
		os.Remove(daemonTmpPath)
		result.Status = "failed"
		result.Error = fmt.Sprintf("daemon checksum verification failed: %v", err)
		return result
	}

	// 3. Make executable
	os.Chmod(cliTmpPath, 0755)
	os.Chmod(daemonTmpPath, 0755)

	// 4. Backup current binaries
	os.Rename("/usr/local/bin/dpm", "/usr/local/bin/dpm.bak")
	os.Rename("/usr/local/bin/dpmd", "/usr/local/bin/dpmd.bak")

	// 5. Atomic swap
	if err := os.Rename(cliTmpPath, "/usr/local/bin/dpm"); err != nil {
		// Rollback
		os.Rename("/usr/local/bin/dpm.bak", "/usr/local/bin/dpm")
		os.Rename("/usr/local/bin/dpmd.bak", "/usr/local/bin/dpmd")
		result.Status = "failed"
		result.Error = fmt.Sprintf("CLI binary swap failed: %v", err)
		return result
	}

	if err := os.Rename(daemonTmpPath, "/usr/local/bin/dpmd"); err != nil {
		// Rollback
		os.Rename("/usr/local/bin/dpm.bak", "/usr/local/bin/dpm")
		os.Rename("/usr/local/bin/dpmd.bak", "/usr/local/bin/dpmd")
		result.Status = "failed"
		result.Error = fmt.Sprintf("daemon binary swap failed: %v", err)
		return result
	}

	// 6. Restart daemon via systemd
	cmd := exec.Command("systemctl", "restart", "dpm")
	if err := cmd.Run(); err != nil {
		// Rollback
		os.Rename("/usr/local/bin/dpm.bak", "/usr/local/bin/dpm")
		os.Rename("/usr/local/bin/dpmd.bak", "/usr/local/bin/dpmd")
		exec.Command("systemctl", "restart", "dpm").Run()

		result.Status = "failed"
		result.Error = fmt.Sprintf("daemon restart failed: %v", err)
		return result
	}

	result.Status = "success"
	result.RollbackAvailable = true
	result.UpgradeTimeMs = time.Since(start).Milliseconds()
	return result
}

// Rollback reverts to the previous DPM binaries.
func Rollback() *Result {
	result := &Result{}

	if _, err := os.Stat("/usr/local/bin/dpm.bak"); os.IsNotExist(err) {
		result.Status = "failed"
		result.Error = "no backup binary found"
		return result
	}

	os.Rename("/usr/local/bin/dpm.bak", "/usr/local/bin/dpm")
	if _, err := os.Stat("/usr/local/bin/dpmd.bak"); err == nil {
		os.Rename("/usr/local/bin/dpmd.bak", "/usr/local/bin/dpmd")
	}

	cmd := exec.Command("systemctl", "restart", "dpm")
	if err := cmd.Run(); err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("daemon restart after rollback failed: %v", err)
		return result
	}

	result.Status = "success"
	return result
}

// downloadFile downloads a URL to a local file.
func downloadFile(path, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

// verifyChecksum downloads checksums file and verifies the binary.
func verifyChecksum(binaryPath, checksumURL, binaryName string) error {
	resp, err := http.Get(checksumURL)
	if err != nil {
		return fmt.Errorf("fetch checksums: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read checksums: %w", err)
	}

	// Find expected checksum
	var expected string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.Contains(line, binaryName) {
			parts := strings.Fields(line)
			if len(parts) >= 1 {
				expected = parts[0]
				break
			}
		}
	}

	if expected == "" {
		return fmt.Errorf("checksum not found for %s", binaryName)
	}

	// Calculate actual checksum
	f, err := os.Open(binaryPath)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	actual := hex.EncodeToString(h.Sum(nil))

	if actual != expected {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, actual)
	}

	return nil
}
