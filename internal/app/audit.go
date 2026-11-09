package app

import (
	"context"
	"fmt"
	"time"

	"subastas-go/internal/domain"
)

func Audit(ctx context.Context, a domain.Auction, start int64, interval time.Duration) <-chan error {
	out := make(chan error, 1)
	go func() {
		defer close(out)
		t := time.NewTicker(interval)
		defer t.Stop()

		last := start
		check := func() error {
			h := a.Highest()
			if h.Amount < last {
				return fmt.Errorf("el monto bajó de %d a %d", last, h.Amount)
			}
			if h.Amount > start && h.Winner == "" {
				return fmt.Errorf("monto %d sin ganador", h.Amount)
			}
			last = h.Amount
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				out <- check()
				return
			case <-t.C:
				if err := check(); err != nil {
					out <- err
					return
				}
			}
		}
	}()
	return out
}
