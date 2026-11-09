package app

import (
	"context"
	"fmt"
	"time"

	"subastas-go/internal/domain"
)

// Audit verifica invariantes leyendo Highest() cada interval, hasta que ctx
// se cancele. Invariantes de cada instantánea:
//  1. el monto nunca baja respecto a la lectura anterior;
//  2. si el monto supera start, hay un ganador (par ganador/monto consistente).
//
// Dueño: la goroutine que lanza Audit. Termina al cancelarse ctx (tras una
// última comprobación) o al detectar la primera violación.
// Canal: lo crea y lo cierra esa misma goroutine (único responsable). Tiene
// capacidad 1 y se envía una sola vez (nil si no hubo violaciones), así que
// el envío nunca bloquea.
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
