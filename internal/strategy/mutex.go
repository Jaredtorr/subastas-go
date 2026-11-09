package strategy

import (
	"sync"

	"subastas-go/internal/domain"
)

// MutexAuction: estrategia A. Un Mutex protege todo el estado de la subasta.
// Invariante: amount solo crece y nunca cambia después de closed = true.
type MutexAuction struct {
	mu     sync.Mutex
	closed bool
	bidder string
	amount int64
}

// NewMutex crea una subasta; las pujas deben superar startPrice.
func NewMutex(startPrice int64) *MutexAuction {
	return &MutexAuction{amount: startPrice}
}

func (a *MutexAuction) Bid(bidder string, amount int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return domain.ErrClosed
	}
	if amount <= a.amount {
		return domain.ErrBidTooLow
	}
	a.bidder, a.amount = bidder, amount
	return nil
}

func (a *MutexAuction) Close() domain.Result {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	return domain.Result{Winner: a.bidder, Amount: a.amount}
}

func (a *MutexAuction) Highest() domain.Result {
	a.mu.Lock()
	defer a.mu.Unlock()
	return domain.Result{Winner: a.bidder, Amount: a.amount}
}
