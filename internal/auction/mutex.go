package auction

import "sync"

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
		return ErrClosed
	}
	if amount <= a.amount {
		return ErrBidTooLow
	}
	a.bidder, a.amount = bidder, amount
	return nil
}

func (a *MutexAuction) Close() Result {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	return Result{Winner: a.bidder, Amount: a.amount}
}

func (a *MutexAuction) Highest() Result {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Result{Winner: a.bidder, Amount: a.amount}
}
