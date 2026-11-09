package auction

import "context"

// Bidder puja sin parar sobre a hasta que ctx se cancele o la subasta cierre.
// Los montos son únicos por postor: id + n*total.
// Termina con ctx.Done() o ErrClosed; no crea goroutines ni canales.
func Bidder(ctx context.Context, a Auction, name string, id, total int64) {
	for n := int64(1); ; n++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := a.Bid(name, id+n*total); err == ErrClosed {
			return
		}
	}
}
