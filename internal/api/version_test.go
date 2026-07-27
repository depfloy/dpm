package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Measured on a production server 2026-07-27: a freshly installed 1.9.0 daemon,
// whose binary reported "dpmd 1.9.0" when asked directly, still made `dpm
// version` print "Daemon: vdev".
//
// handleVersion returned a hardcoded "dev" and the Router's version field —
// which already existed — was never set by NewRouter. So the daemon reported
// "dev" to every caller, on every server, in every release.
//
// The CLI prints its mismatch warning only when the two versions differ, so a
// constant "dev" meant it fired always: the one situation it exists to catch, a
// CLI and daemon left out of step by a partial upgrade, could not be told apart
// from a healthy install. It also makes a fleet version inventory impossible,
// which is the first thing a staged rollout needs.
func TestVersionEndpointReportsTheDaemonVersion(t *testing.T) {
	r := &Router{version: "1.9.0"}

	rec := httptest.NewRecorder()
	r.handleVersion(rec, httptest.NewRequest(http.MethodGet, "/api/v1/version", nil))

	var resp struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
		Meta struct {
			Version string `json:"version"`
		} `json:"meta"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Data.Version != "1.9.0" {
		t.Errorf("data.version = %q, want %q", resp.Data.Version, "1.9.0")
	}

	// Every response carries meta.version, and it was hardcoded in the same way.
	if resp.Meta.Version != "1.9.0" {
		t.Errorf("meta.version = %q, want %q", resp.Meta.Version, "1.9.0")
	}
}

// An unset version must not be reported as a real one. "dev" is what an
// unstamped local build reports, and it stays the honest answer.
func TestVersionEndpointFallsBackToDevWhenUnstamped(t *testing.T) {
	r := &Router{}

	rec := httptest.NewRecorder()
	r.handleVersion(rec, httptest.NewRequest(http.MethodGet, "/api/v1/version", nil))

	var resp struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Data.Version != "dev" {
		t.Errorf("data.version = %q, want %q", resp.Data.Version, "dev")
	}
}
