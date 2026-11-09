package auction

import (
	"context"
	"time"
)

// Run programa el cierre de la subasta a los d de plazo, o antes si ctx se
// cancela (Ctrl+C).
//
// Dueño: la goroutine que lanza Run. Termina cuando vence el plazo o se
// cancela ctx, tras cerrar la subasta.
// Canal: lo crea y lo cierra esa misma goroutine (único responsable). Tiene
// capacidad 1 y se envía una sola vez, así que el envío nunca bloquea.
func Run(ctx context.Context, a Auction, d time.Duration) <-chan Result {
	out := make(chan Result, 1)
	go func() {
		defer close(out)
		tctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		<-tctx.Done()
		out <- a.Close()
	}()
	return out
}
