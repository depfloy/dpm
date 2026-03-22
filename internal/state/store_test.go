package state

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func tempStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestProcessCRUD(t *testing.T) {
	store := tempStore(t)

	ps := &ProcessState{
		Name:          "my-app:0",
		PID:           12345,
		Port:          3000,
		Status:        "online",
		Command:       "npm run start",
		CWD:           "/home/depfloy/42/current",
		Type:          "nodejs",
		Instances:     2,
		RestartPolicy: "always",
		StartedAt:     time.Now(),
		ConfigJSON:    json.RawMessage(`{"name":"my-app"}`),
	}

	// Save
	if err := store.SaveProcess(ps); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Get
	got, err := store.GetProcess("my-app:0")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.PID != 12345 {
		t.Errorf("PID = %d, want 12345", got.PID)
	}
	if got.Port != 3000 {
		t.Errorf("Port = %d, want 3000", got.Port)
	}
	if got.Status != "online" {
		t.Errorf("Status = %s, want online", got.Status)
	}

	// List
	all, err := store.ListProcesses()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("len = %d, want 1", len(all))
	}

	// Update
	ps.Status = "stopped"
	ps.PID = 0
	if err := store.SaveProcess(ps); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = store.GetProcess("my-app:0")
	if got.Status != "stopped" {
		t.Errorf("Status = %s, want stopped", got.Status)
	}

	// Delete
	if err := store.DeleteProcess("my-app:0"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = store.GetProcess("my-app:0")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestProcessNotFound(t *testing.T) {
	store := tempStore(t)

	_, err := store.GetProcess("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent process")
	}
}

func TestPortAllocationCRUD(t *testing.T) {
	store := tempStore(t)

	pa := &PortAllocation{
		Port:        3000,
		ProcessName: "my-app",
		Type:        "nodejs",
		AllocatedAt: time.Now(),
	}

	// Save
	if err := store.SavePortAllocation(pa); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Get
	got, err := store.GetPortAllocation(3000)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ProcessName != "my-app" {
		t.Errorf("ProcessName = %s, want my-app", got.ProcessName)
	}

	// List
	all, err := store.ListPortAllocations()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("len = %d, want 1", len(all))
	}

	// Delete
	if err := store.DeletePortAllocation(3000); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = store.GetPortAllocation(3000)
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestMeta(t *testing.T) {
	store := tempStore(t)

	if err := store.SetMeta("version", "1.0.0"); err != nil {
		t.Fatalf("set: %v", err)
	}

	val, err := store.GetMeta("version")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != "1.0.0" {
		t.Errorf("value = %s, want 1.0.0", val)
	}

	_, err = store.GetMeta("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
}

func TestStoreReopen(t *testing.T) {
	dir := t.TempDir()

	// Open and write
	store1, err := Open(dir)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	store1.SaveProcess(&ProcessState{
		Name:       "persist-test",
		PID:        999,
		Status:     "online",
		ConfigJSON: json.RawMessage(`{}`),
	})
	store1.Close()

	// Reopen and read
	store2, err := Open(dir)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer store2.Close()

	got, err := store2.GetProcess("persist-test")
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if got.PID != 999 {
		t.Errorf("PID = %d, want 999", got.PID)
	}
}

func TestOpenInvalidDir(t *testing.T) {
	// Try opening in a read-only location
	_, err := Open("/proc/nonexistent/dpm")
	if err == nil {
		t.Error("expected error for invalid dir")
	}
}

func TestMultipleProcesses(t *testing.T) {
	store := tempStore(t)

	for i := 0; i < 5; i++ {
		ps := &ProcessState{
			Name:       "app-" + os.Getenv("_"), // unique
			PID:        1000 + i,
			Status:     "online",
			ConfigJSON: json.RawMessage(`{}`),
		}
		ps.Name = "app-" + string(rune('a'+i))
		store.SaveProcess(ps)
	}

	all, err := store.ListProcesses()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 5 {
		t.Errorf("len = %d, want 5", len(all))
	}
}
