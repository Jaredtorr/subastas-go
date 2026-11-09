package app

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"subastas-go/internal/domain"
	"subastas-go/internal/strategy"
)

var auditTargets = map[string]func(int64) domain.Auction{
	"mutex": func(p int64) domain.Auction { return strategy.NewMutex(p) },
	"cas":   func(p int64) domain.Auction { return strategy.NewCAS(p) },
	"actor": func(p int64) domain.Auction { return strategy.NewActor(p) },
}

func TestAuditCleanRun(t *testing.T) {
	defer goleak.VerifyNone(t)
	for name, mk := range auditTargets {
		t.Run(name, func(t *testing.T) {
			a := mk(0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					Bidder(ctx, a, fmt.Sprintf("postor-%d", i), int64(i), 16)
				}()
			}

			auditCtx, stopAudit := context.WithCancel(ctx)
			audit := Audit(auditCtx, a, 0, time.Millisecond)

			res := <-Run(ctx, a, 100*time.Millisecond)
			wg.Wait()
			stopAudit()

			if err := <-audit; err != nil {
				t.Fatalf("violación inesperada: %v", err)
			}
			if a.Highest() != res {
				t.Fatalf("el estado cambió tras el cierre: %+v vs %+v", a.Highest(), res)
			}
		})
	}
}

type decreasingAuction struct{ n atomic.Int64 }

func (d *decreasingAuction) Bid(string, int64) error { return nil }
func (d *decreasingAuction) Close() domain.Result    { return d.Highest() }
func (d *decreasingAuction) Highest() domain.Result {
	return domain.Result{Winner: "x", Amount: 1000 - d.n.Add(1)}
}

func TestAuditDetectsViolation(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	audit := Audit(ctx, &decreasingAuction{}, 0, time.Millisecond)
	select {
	case err := <-audit:
		if err == nil {
			t.Fatal("el auditor debía detectar que el monto baja")
		}
	case <-time.After(time.Second):
		t.Fatal("el auditor no detectó la violación en 1 s")
	}
}
