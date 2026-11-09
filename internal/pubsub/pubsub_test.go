package pubsub

import (
	"sync"
	"testing"

	"go.uber.org/goleak"

	"subastas-go/internal/domain"
)

// testAuction es una implementación mínima de domain.Auction para probar Notifying.
type testAuction struct {
	bidder string
	amount int64
	closed bool
}

func (a *testAuction) Bid(bidder string, amount int64) error {
	if a.closed {
		return domain.ErrClosed
	}
	if amount <= a.amount {
		return domain.ErrBidTooLow
	}
	a.bidder, a.amount = bidder, amount
	return nil
}

func (a *testAuction) Close() domain.Result {
	a.closed = true
	return domain.Result{Winner: a.bidder, Amount: a.amount}
}

func (a *testAuction) Highest() domain.Result {
	return domain.Result{Winner: a.bidder, Amount: a.amount}
}

// Un suscriptor que nunca lee se desconecta; el rápido recibe todo.
func TestBrokerSlowSubscriberDropped(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBroker()
	slow := b.Subscribe(4) // nunca lee mientras se publica
	fast := b.Subscribe(1000)

	const n = 100
	for i := 1; i <= n; i++ {
		b.Publish(Event{Kind: EventNewBid, Amount: int64(i)})
	}
	b.Close()

	gotFast := 0
	for range fast.C {
		gotFast++
	}
	gotSlow := 0
	for range slow.C { // termina porque el broker cerró su canal
		gotSlow++
	}
	if gotFast != n {
		t.Fatalf("el suscriptor rápido recibió %d, se esperaban %d", gotFast, n)
	}
	if gotSlow != 4 {
		t.Fatalf("el lento recibió %d, se esperaban 4 (su cola)", gotSlow)
	}
}

// Notifying publica pujas aceptadas y un único evento de cierre, en orden.
func TestNotifyingPublishesBidsAndClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBroker()
	sub := b.Subscribe(16)
	a := NewNotifying(&testAuction{}, b)

	_ = a.Bid("ana", 10)
	_ = a.Bid("luis", 5) // rechazada: no genera evento
	_ = a.Bid("luis", 20)
	a.Close()
	a.Close() // idempotente: no repite el evento
	b.Close()

	var got []Event
	for ev := range sub.C {
		got = append(got, ev)
	}
	want := []Event{
		{EventNewBid, "ana", 10},
		{EventNewBid, "luis", 20},
		{EventClosed, "luis", 20},
	}
	if len(got) != len(want) {
		t.Fatalf("eventos: %+v, se esperaban %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("evento %d: %+v, se esperaba %+v", i, got[i], want[i])
		}
	}
}

// Cerrar el broker con publicadores y suscriptores activos no se cuelga,
// no entra en pánico y no deja goroutines.
func TestBrokerConcurrentPublishAndClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBroker()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		s := b.Subscribe(8)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range s.C {
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				b.Publish(Event{Kind: EventNewBid, Amount: int64(j)})
			}
		}()
	}
	b.Close()
	wg.Wait()
}
