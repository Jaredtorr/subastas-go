package strategy

import (
	"sync/atomic"

	"subastas-go/internal/domain"
)

type state struct {
	bidder string
	amount int64
	closed bool
}

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
			return domain.ErrClosed
		}
		if amount <= cur.amount {
			return domain.ErrBidTooLow
		}
		if a.st.CompareAndSwap(cur, &state{bidder: bidder, amount: amount}) {
			return nil
		}
	}
}

func (a *CASAuction) Close() domain.Result {
	for {
		cur := a.st.Load()
		if cur.closed {
			return domain.Result{Winner: cur.bidder, Amount: cur.amount}
		}
		if a.st.CompareAndSwap(cur, &state{bidder: cur.bidder, amount: cur.amount, closed: true}) {
			return domain.Result{Winner: cur.bidder, Amount: cur.amount}
		}
	}
}

func (a *CASAuction) Highest() domain.Result {
	cur := a.st.Load()
	return domain.Result{Winner: cur.bidder, Amount: cur.amount}
}
