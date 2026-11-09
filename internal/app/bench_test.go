package app

import (
	"fmt"
	"sync"
	"testing"
)

// Mide el costo de una puja con 1, 4, 16 y 64 postores compitiendo por la
// misma subasta, para cada estrategia. ns/op es el costo medio por puja.
func BenchmarkBid(b *testing.B) {
	for name, mk := range auditTargets {
		for _, n := range []int{1, 4, 16, 64} {
			b.Run(fmt.Sprintf("%s/bidders=%d", name, n), func(b *testing.B) {
				b.ReportAllocs()
				a := mk(0)
				defer a.Close()

				per := b.N / n
				if per == 0 {
					per = 1
				}
				var wg sync.WaitGroup
				b.ResetTimer()
				for i := 0; i < n; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						who := fmt.Sprintf("postor-%d", i)
						for j := 1; j <= per; j++ {
							_ = a.Bid(who, int64(i)+int64(j)*int64(n))
						}
					}()
				}
				wg.Wait()
				b.StopTimer()
			})
		}
	}
}