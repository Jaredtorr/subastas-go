// Package domain implementa subastas concurrentes con distintas estrategias.
package domain

import "errors"

var (
	ErrBidTooLow = errors.New("puja menor o igual a la máxima actual")
	ErrClosed    = errors.New("subasta cerrada")
)

// Result es el resultado final de una subasta.
type Result struct {
	Winner string // vacío si nadie pujó
	Amount int64
}

// Auction es la interfaz común a todas las estrategias (A, B y C),
// para poder compararlas con las mismas pruebas y benchmarks.
type Auction interface {
	// Bid intenta registrar una puja. Devuelve ErrBidTooLow o ErrClosed.
	Bid(bidder string, amount int64) error
	// Close cierra la subasta y devuelve el ganador. Es idempotente.
	Close() Result
	// Highest devuelve la puja máxima actual.
	Highest() Result
}
