package app

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

var bidderCounts = []int{1, 4, 16, 64}

// BenchmarkBid: carga "mixta". Cada postor puja i + j*n por su cuenta, sin
// coordinarse, así que los que se atrasan son rechazados. Se reporta la tasa
// de aceptación para saber qué camino se está midiendo.
func BenchmarkBid(b *testing.B) {
	runBid(b, func(i, j, n int, _ *atomic.Int64) int64 {
		return int64(i) + int64(j)*int64(n)
	})
}

// BenchmarkBidRising: carga "creciente". Los montos salen de un contador
// compartido, así que casi todas las pujas son aceptadas y se mide el camino
// de aceptación bajo contención. El contador suma un costo fijo parecido a
// todas las estrategias: compáralas entre sí, no contra BenchmarkBid.
func BenchmarkBidRising(b *testing.B) {
	runBid(b, func(_, _, _ int, next *atomic.Int64) int64 {
		return next.Add(1)
	})
}

// runBid mide el costo por puja (tiempo de pared / pujas totales, es decir,
// rendimiento y no latencia) con 1, 4, 16 y 64 postores compitiendo por la
// misma subasta, para cada estrategia.
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
