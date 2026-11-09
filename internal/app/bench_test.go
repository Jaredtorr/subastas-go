package app

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

var bidderCounts = []int{1, 4, 16, 64}

func BenchmarkBid(b *testing.B) {
	runBid(b, func(i, j, n int, _ *atomic.Int64) int64 {
		return int64(i) + int64(j)*int64(n)
	})
}

func BenchmarkBidRising(b *testing.B) {
	runBid(b, func(_, _, _ int, next *atomic.Int64) int64 {
		return next.Add(1)
	})
}

func runBid(b *testing.B, amount func(i, j, n int, next *atomic.Int64) int64) {
	for name, mk := range auditTargets {
		for _, n := range bidderCounts {
			b.Run(fmt.Sprintf("strategy=%s/bidders=%d", name, n), func(b *testing.B) {
				b.ReportAllocs()
				a := mk(0)
				defer a.Close()

				per := b.N / n
				if per == 0 {
					per = 1
				}
				var next, accepted atomic.Int64
				var wg sync.WaitGroup
				b.ResetTimer()
				for i := 0; i < n; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						who := fmt.Sprintf("postor-%d", i)
						var ok int64
						for j := 1; j <= per; j++ {
							if a.Bid(who, amount(i, j, n, &next)) == nil {
								ok++
							}
						}
						accepted.Add(ok)
					}()
				}
				wg.Wait()
				b.StopTimer()
				b.ReportMetric(float64(accepted.Load())/float64(per*n), "aceptadas/op")
			})
		}
	}
}
