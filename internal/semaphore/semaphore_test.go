package semaphore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSemaphoreRespectsLimit(t *testing.T) {
	const limit = 3
	s := New(limit)

	for i := 0; i < limit; i++ {
		if err := s.Acquire(context.Background()); err != nil {
			t.Fatalf("adquisición %d: %v", i, err)
		}
	}

	if s.TryAcquire() {
		t.Fatal("no debería quedar ningún permiso libre")
	}

	for i := 0; i < limit; i++ {
		s.Release()
	}
	if s.Len() != 0 {
		t.Fatalf("permisos ocupados = %d, se esperaba 0", s.Len())
	}

	if !s.TryAcquire() {
		t.Fatal("el permiso liberado debería poder adquirirse")
	}
	s.Release()
	if s.Cap() != limit {
		t.Fatalf("límite = %d, se esperaba %d", s.Cap(), limit)
	}
}

func TestSemaphoreAcquireCancel(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, se esperaba context.Canceled", err)
	}
}

func TestSemaphoreConcurrentLimit(t *testing.T) {
	const limit = 4
	s := New(limit)

	var active, maxActive atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Acquire(context.Background()); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer s.Release()

			now := active.Add(1)
			for {
				old := maxActive.Load()
				if now <= old || maxActive.CompareAndSwap(old, now) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
		}()
	}

	wg.Wait()
	if got := maxActive.Load(); got > limit {
		t.Fatalf("concurrencia máxima = %d, límite = %d", got, limit)
	}
	if got := maxActive.Load(); got == 0 {
		t.Fatal("no se ejecutó ninguna operación concurrente")
	}
}
