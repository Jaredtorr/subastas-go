package app

import (
	"context"

	"subastas-go/internal/domain"
	"subastas-go/internal/metrics"
)

func Bidder(ctx context.Context, a domain.Auction, name string, id, total int64) {
	metrics.Default.BidderStarted()
	defer metrics.Default.BidderFinished()

	for n := int64(1); ; n++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := a.Bid(name, id+n*total); err == domain.ErrClosed {
			return
		}
	}
}
