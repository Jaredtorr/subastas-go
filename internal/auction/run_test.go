package auction

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func launch(ctx context.Context, a Auction, n int) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Bidder(ctx, a, fmt.Sprintf("postor-%d", i), int64(i), int64(n))
		}()
	}
	return &wg
}

// Al vencer el plazo, la subasta cierra, el ganador es la puja más alta
// y no quedan goroutines vivas.
func TestRunDeadline(t *testing.T) {
	defer goleak.VerifyNone(t)
	for name, mk := range strategies {
		t.Run(name, func(t *testing.T) {
			a := mk(0)
			wg := launch(context.Background(), a, 32)
			res := <-Run(context.Background(), a, 50*time.Millisecond)
			wg.Wait()

			if res.Amount == 0 || res.Winner == "" {
				t.Fatalf("sin ganador: %+v", res)
			}
			if a.Highest() != res {
				t.Fatalf("el estado cambió tras el cierre: %+v vs %+v", a.Highest(), res)
			}
			if err := a.Bid("tarde", res.Amount+1); err != ErrClosed {
				t.Fatalf("puja tras el cierre debe dar ErrClosed, got %v", err)
			}
		})
	}
}

// Cancelar el contexto padre (Ctrl+C) cierra todo en menos de 1 s.
func TestRunCancel(t *testing.T) {
	defer goleak.VerifyNone(t)
	for name, mk := range strategies {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			a := mk(0)
			wg := launch(ctx, a, 16)
			done := Run(ctx, a, time.Hour)

			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("la subasta no cerró en 1 s tras cancelar")
			}
			wg.Wait()
		})
	}
}
