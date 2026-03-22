package port

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/depfloy/dpm/internal/state"
	"github.com/depfloy/dpm/pkg/config"
)

// Manager handles port allocation and conflict detection.
type Manager struct {
	mu     sync.Mutex
	store  *state.Store
	ranges config.PortRanges
}

// NewManager creates a new port manager.
func NewManager(store *state.Store, ranges config.PortRanges) *Manager {
	return &Manager{
		store:  store,
		ranges: ranges,
	}
}

// Allocate finds and assigns available ports for a process.
// Returns the allocated ports. count specifies how many ports are needed.
func (m *Manager) Allocate(processName, processType string, count int) ([]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rangeStart, rangeEnd := m.rangeForType(processType)

	// Get all current allocations
	allocated, err := m.store.ListPortAllocations()
	if err != nil {
		return nil, fmt.Errorf("list allocations: %w", err)
	}

	usedPorts := make(map[int]bool)
	for _, a := range allocated {
		usedPorts[a.Port] = true
	}

	var ports []int
	for port := rangeStart; port <= rangeEnd && len(ports) < count; port++ {
		if usedPorts[port] {
			continue
		}

		// Verify port is actually free (not used by external process)
		if !isPortFree(port) {
			continue
		}

		ports = append(ports, port)
	}

	if len(ports) < count {
		return nil, fmt.Errorf("not enough free ports in range %d-%d (need %d, found %d)",
			rangeStart, rangeEnd, count, len(ports))
	}

	// Persist allocations
	for _, port := range ports {
		pa := &state.PortAllocation{
			Port:        port,
			ProcessName: processName,
			Type:        processType,
			AllocatedAt: time.Now(),
		}
		if err := m.store.SavePortAllocation(pa); err != nil {
			return nil, fmt.Errorf("save allocation for port %d: %w", port, err)
		}
	}

	return ports, nil
}

// AllocateSpecific assigns a specific port to a process.
func (m *Manager) AllocateSpecific(processName, processType string, port int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already allocated
	existing, err := m.store.GetPortAllocation(port)
	if err == nil && existing.ProcessName != processName {
		return fmt.Errorf("port %d already allocated to %s", port, existing.ProcessName)
	}

	if !isPortFree(port) {
		return fmt.Errorf("port %d is in use by another process", port)
	}

	pa := &state.PortAllocation{
		Port:        port,
		ProcessName: processName,
		Type:        processType,
		AllocatedAt: time.Now(),
	}
	return m.store.SavePortAllocation(pa)
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

// GetByProcess returns all ports allocated to a process.
func (m *Manager) GetByProcess(processName string) ([]int, error) {
	allocations, err := m.store.ListPortAllocations()
	if err != nil {
		return nil, err
	}

	var ports []int
	for _, a := range allocations {
		if a.ProcessName == processName {
			ports = append(ports, a.Port)
		}
	}
	return ports, nil
}

// rangeForType returns the port range for a process type.
func (m *Manager) rangeForType(processType string) (int, int) {
	switch processType {
	case "nodejs":
		return m.ranges.NodeJS[0], m.ranges.NodeJS[1]
	case "plugins", "php":
		return m.ranges.Plugins[0], m.ranges.Plugins[1]
	case "worker":
		return m.ranges.Workers[0], m.ranges.Workers[1]
	default:
		return m.ranges.NodeJS[0], m.ranges.NodeJS[1]
	}
}

// isPortFree checks if a port is available by attempting to listen on it.
func isPortFree(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}
