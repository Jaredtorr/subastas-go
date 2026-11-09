package semaphore

import (
	"context"
	"fmt"
	"sync"

	"subastas-go/internal/metrics"
)

type Semaphore struct {
	permits chan struct{}
	once    sync.Once
}

func New(limit int) *Semaphore {
	if limit < 1 {
		limit = 1
	}
	s := &Semaphore{permits: make(chan struct{}, limit)}
	metrics.Default.AddSemaphoreCapacity(limit)
	return s
}

func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.permits <- struct{}{}:
		metrics.Default.SemaphoreAcquired()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Semaphore) TryAcquire() bool {
	select {
	case s.permits <- struct{}{}:
		metrics.Default.SemaphoreAcquired()
		return true
	default:
		return false
	}
}

func (s *Semaphore) Release() {
	select {
	case <-s.permits:
		metrics.Default.SemaphoreReleased()
	default:
		panic(fmt.Sprintf("semaphore: Release sin permiso adquirido (capacidad %d)", cap(s.permits)))
	}
}

func (s *Semaphore) Close() {
	s.once.Do(func() {
		metrics.Default.AddSemaphoreCapacity(-cap(s.permits))
	})
}

func (s *Semaphore) Len() int {
	return len(s.permits)
}

func (s *Semaphore) Cap() int {
	return cap(s.permits)
}
