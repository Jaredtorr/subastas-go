package auction

import (
	"fmt"
	"sync"
	"testing"
)

// Muchos postores pujan montos distintos a la vez: gana siempre el máximo.
func TestMutexConcurrentBids(t *testing.T) {
	const bidders, perBidder = 64, 200
	a := NewMutex(0)

	var wg sync.WaitGroup
	for i := 0; i < bidders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("postor-%d", i)
			for j := 1; j <= perBidder; j++ {
				_ = a.Bid(name, int64(i*perBidder+j))
			}
		}()
	}
	wg.Wait()

	want := int64(bidders * perBidder)
	if got := a.Close(); got.Amount != want {
		t.Fatalf("ganador con %d, se esperaba %d", got.Amount, want)
	}
}

func TestMutexRejectsLowAndClosed(t *testing.T) {
	a := NewMutex(10)
	if err := a.Bid("ana", 10); err != ErrBidTooLow {
		t.Fatalf("igual al precio base debe rechazarse, got %v", err)
	}
	if err := a.Bid("ana", 20); err != nil {
		t.Fatal(err)
	}
	if err := a.Bid("luis", 20); err != ErrBidTooLow {
		t.Fatalf("puja igual debe rechazarse, got %v", err)
	}
	res := a.Close()
	if res.Winner != "ana" || res.Amount != 20 {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if err := a.Bid("luis", 99); err != ErrClosed {
		t.Fatalf("puja tras cierre debe rechazarse, got %v", err)
	}
	if a.Close() != res {
		t.Fatal("Close debe ser idempotente")
	}
}
