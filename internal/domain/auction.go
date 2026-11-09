package domain

import "errors"

var (
	ErrBidTooLow = errors.New("puja menor o igual a la máxima actual")
	ErrClosed    = errors.New("subasta cerrada")
)

type Result struct {
	Winner string
	Amount int64
}

type Auction interface {
	Bid(bidder string, amount int64) error
	Close() Result
	Highest() Result
}

type Lot struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Category    string `json:"category"`
	Description string `json:"description"`
	Image       string `json:"image"`
}
