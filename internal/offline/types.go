package offline

import (
	"sync"
	"time"
)

// TaskStatus lifecycle: pending -> running <-> paused -> done | failed
type TaskStatus string

const (
	StatusPending TaskStatus = "pending"
	StatusRunning TaskStatus = "running"
	StatusPaused  TaskStatus = "paused"
	StatusDone    TaskStatus = "done"
	StatusFailed  TaskStatus = "failed"
)

// Task tracks one offline-download job. In PoC scope it lives in memory only.
type Task struct {
	ID            string     `json:"id"`
	SourceURL     string     `json:"source_url"`
	VirtualBucket string     `json:"virtual_bucket,omitempty"`
	Key           string     `json:"key"`
	R2Bucket      string     `json:"r2_bucket"`
	Size          int64      `json:"size"`
	PartSize      int64      `json:"part_size"`
	PartsTotal    int        `json:"parts_total"`
	PartsDone     int        `json:"parts_done"`
	Status        TaskStatus `json:"status"`
	Error         string     `json:"error,omitempty"`
	ETag          string     `json:"etag,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// Store is an in-memory task registry with mutex. Good enough for PoC.
type Store struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

func NewStore() *Store {
	return &Store{tasks: make(map[string]*Task)}
}

func (s *Store) Put(t *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.UpdatedAt = time.Now()
	s.tasks[t.ID] = t
}

func (s *Store) Get(id string) (*Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	return &cp, true
}

func (s *Store) List() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		cp := *t
		out = append(out, &cp)
	}
	return out
}

func (s *Store) Update(id string, mutate func(*Task)) (*Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	mutate(t)
	t.UpdatedAt = time.Now()
	cp := *t
	return &cp, true
}
