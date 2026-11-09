package app

import (
	"context"
	"time"

	"subastas-go/internal/domain"
)

func Run(ctx context.Context, a domain.Auction, d time.Duration) <-chan domain.Result {
	out := make(chan domain.Result, 1)
	go func() {
		defer close(out)
		tctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		<-tctx.Done()
		out <- a.Close()
	}()
	return out
}
