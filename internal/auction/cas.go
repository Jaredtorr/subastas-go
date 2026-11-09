package auction

import "sync/atomic"

// state es inmutable: nunca se modifica, solo se reemplaza completo.
type state struct {
	bidder string
	amount int64
	closed bool
}

// CASAuction: estrategia B. Sin candados: el estado vive en un puntero
// atómico y se reemplaza con CompareAndSwap en un bucle de reintento.
type CASAuction struct {
	st atomic.Pointer[state]
}

func NewCAS(startPrice int64) *CASAuction {
	a := &CASAuction{}
	a.st.Store(&state{amount: startPrice})
	return a
}

func (a *CASAuction) Bid(bidder string, amount int64) error {
	for {
		cur := a.st.Load()
		if cur.closed {
			return ErrClosed
		}
		if amount <= cur.amount {
			return ErrBidTooLow
		}
		// Si otro cambió el estado entre Load y CAS, el CAS falla y reintentamos.
		if a.st.CompareAndSwap(cur, &state{bidder: bidder, amount: amount}) {
			return nil
		}
	}
}

func (a *CASAuction) Close() Result {
	for {
		cur := a.st.Load()
		if cur.closed {
			return Result{Winner: cur.bidder, Amount: cur.amount}
		}
		if a.st.CompareAndSwap(cur, &state{bidder: cur.bidder, amount: cur.amount, closed: true}) {
			return Result{Winner: cur.bidder, Amount: cur.amount}
		}
	}
}

func (a *CASAuction) Highest() Result {
	cur := a.st.Load()
	return Result{Winner: cur.bidder, Amount: cur.amount}
}