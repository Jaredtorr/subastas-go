package workpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestPoolLimitsWorkersAndCompletesJobs(t *testing.T) {
	defer goleak.VerifyNone(t)

	const size, total = 3, 30
	pool := New(size)

	var active, maxActive atomic.Int64
	var completed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(total)

	for i := 0; i < total; i++ {
		err := pool.Submit(context.Background(), func(context.Context) error {
			defer wg.Done()

			now := active.Add(1)
			for {
				old := maxActive.Load()
				if now <= old || maxActive.CompareAndSwap(old, now) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			completed.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	wg.Wait()
	pool.Close()

	if got := completed.Load(); got != total {
		t.Fatalf("trabajos completados = %d, se esperaban %d", got, total)
	}
	if got := maxActive.Load(); got > size {
		t.Fatalf("concurrencia máxima = %d, límite = %d", got, size)
	}
}

func TestPoolSubmitAfterClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	pool := New(2)
	pool.Close()
	pool.Close()

	err := pool.Submit(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, se esperaba ErrClosed", err)
	}
}

func TestPoolSubmitCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	pool := New(1)
	defer pool.Close()

	block := make(chan struct{})
	if err := pool.Submit(context.Background(), func(context.Context) error {
		<-block
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pool.Submit(ctx, func(context.Context) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, se esperaba context.Canceled", err)
	}

	close(block)
}

func TestPoolJobSeesContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	pool := New(1)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := make(chan struct{})

	if err := pool.Submit(ctx, func(jobCtx context.Context) error {
		close(started)
		<-jobCtx.Done()
		done <- jobCtx.Err()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, se esperaba context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("el trabajo no recibió la cancelación")
	}
}
