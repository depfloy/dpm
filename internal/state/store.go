package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketProcesses = []byte("processes")
	bucketPorts     = []byte("ports")
	bucketMeta      = []byte("meta")
)

// Store provides persistent state storage backed by BoltDB.
type Store struct {
	db   *bolt.DB
	path string
}

// ProcessState represents the persisted state of a managed process.
type ProcessState struct {
	Name          string            `json:"name"`
	PID           int               `json:"pid"`
	Port          int               `json:"port"`
	SecondaryPort int               `json:"secondary_port,omitempty"`
	Status        string            `json:"status"` // online, stopped, errored, starting
	Command       string            `json:"command"`
	CWD           string            `json:"cwd"`
	Type          string            `json:"type"`
	Env           map[string]string `json:"env,omitempty"`
	Instances     int               `json:"instances"`
	RestartPolicy string            `json:"restart_policy"`
	RestartCount  int               `json:"restart_count"`
	MaxRestarts   int               `json:"max_restarts,omitempty"`
	MaxMemory     string            `json:"max_memory,omitempty"`
	StartedAt     time.Time         `json:"started_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	ConfigJSON    json.RawMessage   `json:"config_json"` // Full ProcessConfig as JSON
}

// PortAllocation represents a port assignment.
type PortAllocation struct {
	Port        int    `json:"port"`
	ProcessName string `json:"process_name"`
	Type        string `json:"type"` // nodejs, plugins, workers
	AllocatedAt time.Time `json:"allocated_at"`
}

// Open creates or opens a BoltDB store at the given directory.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	dbPath := filepath.Join(dir, "dpm.db")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	// Create buckets
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{bucketProcesses, bucketPorts, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init buckets: %w", err)
	}

	return &Store{db: db, path: dbPath}, nil
}

// Close closes the BoltDB store.
func (s *Store) Close() error {
	return s.db.Close()
}

// SaveProcess persists a process state.
func (s *Store) SaveProcess(ps *ProcessState) error {
	ps.UpdatedAt = time.Now()
	data, err := json.Marshal(ps)
	if err != nil {
		return fmt.Errorf("marshal process state: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketProcesses).Put([]byte(ps.Name), data)
	})
}

// GetProcess retrieves a process state by name.
func (s *Store) GetProcess(name string) (*ProcessState, error) {
	var ps ProcessState
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketProcesses).Get([]byte(name))
		if data == nil {
			return fmt.Errorf("process not found: %s", name)
		}
		return json.Unmarshal(data, &ps)
	})
	if err != nil {
		return nil, err
	}
	return &ps, nil
}

// ListProcesses returns all persisted process states.
func (s *Store) ListProcesses() ([]*ProcessState, error) {
	var processes []*ProcessState
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketProcesses)
		return b.ForEach(func(k, v []byte) error {
			var ps ProcessState
			if err := json.Unmarshal(v, &ps); err != nil {
				return err
			}
			processes = append(processes, &ps)
			return nil
		})
	})
	return processes, err
}

// DeleteProcess removes a process state.
func (s *Store) DeleteProcess(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketProcesses).Delete([]byte(name))
	})
}

// SavePortAllocation persists a port allocation.
func (s *Store) SavePortAllocation(pa *PortAllocation) error {
	data, err := json.Marshal(pa)
	if err != nil {
		return fmt.Errorf("marshal port allocation: %w", err)
	}
	key := fmt.Sprintf("%d", pa.Port)
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPorts).Put([]byte(key), data)
	})
}

// GetPortAllocation retrieves a port allocation.
func (s *Store) GetPortAllocation(port int) (*PortAllocation, error) {
	var pa PortAllocation
	key := fmt.Sprintf("%d", port)
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketPorts).Get([]byte(key))
		if data == nil {
			return fmt.Errorf("port not allocated: %d", port)
		}
		return json.Unmarshal(data, &pa)
	})
	if err != nil {
		return nil, err
	}
	return &pa, nil
}

// ListPortAllocations returns all port allocations.
func (s *Store) ListPortAllocations() ([]*PortAllocation, error) {
	var allocations []*PortAllocation
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPorts).ForEach(func(k, v []byte) error {
			var pa PortAllocation
			if err := json.Unmarshal(v, &pa); err != nil {
				return err
			}
			allocations = append(allocations, &pa)
			return nil
		})
	})
	return allocations, err
}

// DeletePortAllocation removes a port allocation.
func (s *Store) DeletePortAllocation(port int) error {
	key := fmt.Sprintf("%d", port)
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPorts).Delete([]byte(key))
	})
}

// SetMeta stores a key-value pair in the meta bucket.
func (s *Store) SetMeta(key, value string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(key), []byte(value))
	})
}

// GetMeta retrieves a meta value by key.
func (s *Store) GetMeta(key string) (string, error) {
	var value string
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketMeta).Get([]byte(key))
		if data == nil {
			return fmt.Errorf("meta key not found: %s", key)
		}
		value = string(data)
		return nil
	})
	return value, err
}
