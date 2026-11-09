package auction

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

var (
	_ Auction = (*MutexAuction)(nil)
	_ Auction = (*CASAuction)(nil)
)

var strategies = map[string]func(startPrice int64) Auction{
	"mutex": func(p int64) Auction { return NewMutex(p) },
	"cas":   func(p int64) Auction { return NewCAS(p) },
}

// Muchos postores sobre la misma subasta: gana siempre el monto máximo.
func TestStrategiesConcurrentBids(t *testing.T) {
	for name, mk := range strategies {
		t.Run(name, func(t *testing.T) {
			const bidders, perBidder = 64, 200
			a := mk(0)

			var wg sync.WaitGroup
			for i := 0; i < bidders; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					who := fmt.Sprintf("postor-%d", i)
					for j := 1; j <= perBidder; j++ {
						_ = a.Bid(who, int64(i*perBidder+j))
					}
				}()
			}
			wg.Wait()

			if got := a.Close(); got.Amount != bidders*perBidder {
				t.Fatalf("ganó %d, se esperaba %d", got.Amount, bidders*perBidder)
			}
		})
	}
}

// Pujas compitiendo con el cierre: el ganador debe ser exactamente la
// puja más alta ACEPTADA, y nada se acepta después de Close.
func TestStrategiesBidsRaceWithClose(t *testing.T) {
	for name, mk := range strategies {
		t.Run(name, func(t *testing.T) {
			const bidders = 16
			a := mk(0)

			var accepted atomic.Int64
			maxOK := make([]int64, bidders) // cada goroutine escribe solo su índice
			var wg sync.WaitGroup
			for i := 0; i < bidders; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					who := fmt.Sprintf("postor-%d", i)
					for j := int64(0); ; j++ {
						amount := j*bidders + int64(i) + 1 // montos únicos por postor
						switch err := a.Bid(who, amount); err {
						case nil:
							maxOK[i] = amount
							accepted.Add(1)
						case ErrClosed:
							return
						}
					}
				}()
			}

			for accepted.Load() < 100 { // espera sin time.Sleep
				runtime.Gosched()
			}
			res := a.Close()
			wg.Wait()

			var best int64
			for _, m := range maxOK {
				best = max(best, m)
			}
			if res.Amount != best {
				t.Fatalf("Close devolvió %d pero la puja aceptada más alta fue %d", res.Amount, best)
			}
			if a.Highest() != res {
				t.Fatalf("el estado cambió tras cerrar: %+v vs %+v", a.Highest(), res)
			}
		})
	}
}

func TestCASRejectsLowAndClosed(t *testing.T) {
	a := NewCAS(10)
	if err := a.Bid("ana", 10); err != ErrBidTooLow {
		t.Fatalf("got %v", err)
	}
	if err := a.Bid("ana", 20); err != nil {
		t.Fatal(err)
	}
	res := a.Close()
	if res.Winner != "ana" || res.Amount != 20 {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if err := a.Bid("luis", 99); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
}