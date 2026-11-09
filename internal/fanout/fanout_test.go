package fanout

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestStartCollectsResultsAndCloses(t *testing.T) {
	defer goleak.VerifyNone(t)

	items := []int{1, 2, 3, 4, 5, 6, 7, 8}
	out := Start(context.Background(), items, 3, func(_ context.Context, n int) (int, error) {
		return n * n, nil
	})

	got := make([]int, len(items))
	seen := make([]bool, len(items))
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case result, open := <-out:
			if !open {
				for i, ok := range seen {
					if !ok {
						t.Fatalf("falta el resultado %d", i)
					}
				}
				for i, want := range items {
					if got[i] != want*want {
						t.Fatalf("resultado %d = %d, se esperaba %d", i, got[i], want*want)
					}
				}
				return
			}
			if result.Err != nil {
				t.Fatalf("error inesperado: %v", result.Err)
			}
			got[result.Index] = result.Value
			seen[result.Index] = true
		case <-timeout.C:
			t.Fatal("el canal de resultados no se cerró")
		}
	}
}

func TestStartRespectsConcurrencyLimit(t *testing.T) {
	defer goleak.VerifyNone(t)

	const limit = 3
	var active, maxActive atomic.Int64

	items := make([]int, 30)
	for i := range items {
		items[i] = i
	}

	_, err := Run(context.Background(), items, limit, func(ctx context.Context, n int) (int, error) {
		now := active.Add(1)
		for {
			old := maxActive.Load()
			if now <= old || maxActive.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		active.Add(-1)
		return n, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := maxActive.Load(); got > limit {
		t.Fatalf("concurrencia máxima = %d, límite = %d", got, limit)
	}
}

func TestStartCancelClosesChannel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	items := []int{1, 2, 3, 4, 5}

	out := Start(ctx, items, 2, func(ctx context.Context, _ int) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})

	cancel()

	select {
	case _, open := <-out:
		if open {
			for range out {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("el canal de resultados no se cerró tras cancelar")
	}
}

func TestRunPropagatesErrorAndCancelsRest(t *testing.T) {
	defer goleak.VerifyNone(t)

	boom := errors.New("fallo de validación")

	items := []int{0, 1, 2, 3, 4, 5}
	start := time.Now()
	_, err := Run(context.Background(), items, len(items), func(ctx context.Context, n int) (int, error) {
		if n == 0 {
			return 0, boom
		}
		<-ctx.Done()
		return 0, ctx.Err()
	})

	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, se esperaba %v", err, boom)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("la cancelación tardó demasiado: %s", elapsed)
	}
}
