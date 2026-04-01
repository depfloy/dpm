package port

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/depfloy/dpm/internal/state"
)

// Manager handles port validation and tracking.
// Port allocation is done by Depfloy (PHP) - DPM only validates and tracks.
type Manager struct {
	mu    sync.Mutex
	store *state.Store
}

// NewManager creates a new port manager.
func NewManager(store *state.Store) *Manager {
	return &Manager{
		store: store,
	}
}

// Release frees all ports allocated to a process.
func (m *Manager) Release(processName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	allocations, err := m.store.ListPortAllocations()
	if err != nil {
		return err
	}

	for _, a := range allocations {
		if a.ProcessName == processName {
			if err := m.store.DeletePortAllocation(a.Port); err != nil {
				return fmt.Errorf("release port %d: %w", a.Port, err)
			}
		}
	}

	return nil
}

// ReleasePort frees a single port.
func (m *Manager) ReleasePort(port int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store.DeletePortAllocation(port)
}

// List returns all current port allocations.
func (m *Manager) List() ([]*state.PortAllocation, error) {
	return m.store.ListPortAllocations()
}

// IsPortFree checks if a port is available (exported for use by API layer).
func (m *Manager) IsPortFree(port int) bool {
	return isPortFree(port)
}

// isPortFree checks if a port is available by attempting to listen on it with a timeout.
func isPortFree(port int) bool {
	lc := net.ListenConfig{}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}
