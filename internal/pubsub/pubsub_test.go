package pubsub

import (
	"sync"
	"testing"

	"go.uber.org/goleak"

	"subastas-go/internal/domain"
)

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

func TestBrokerSlowSubscriberDropped(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBroker()
	slow := b.Subscribe(4)
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
	for range slow.C {
		gotSlow++
	}
	if gotFast != n {
		t.Fatalf("el suscriptor rápido recibió %d, se esperaban %d", gotFast, n)
	}
	if gotSlow != 4 {
		t.Fatalf("el lento recibió %d, se esperaban 4 (su cola)", gotSlow)
	}
}

func TestNotifyingPublishesBidsAndClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBroker()
	sub := b.Subscribe(16)
	a := NewNotifying(&testAuction{}, b)

	_ = a.Bid("ana", 10)
	_ = a.Bid("luis", 5)
	_ = a.Bid("luis", 20)
	a.Close()
	a.Close()
	b.Close()

	var got []Event
	for ev := range sub.C {
		got = append(got, ev)
	}
	want := []Event{
		{Kind: EventNewBid, Bidder: "ana", Amount: 10},
		{Kind: EventNewBid, Bidder: "luis", Amount: 20},
		{Kind: EventClosed, Bidder: "luis", Amount: 20},
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

func TestBrokerUnsubscribe(t *testing.T) {
	defer goleak.VerifyNone(t)

	b := NewBroker()
	sub := b.Subscribe(4)

	b.Unsubscribe(sub)
	b.Unsubscribe(sub)

	if _, ok := <-sub.C; ok {
		t.Fatal("el canal debería estar cerrado")
	}

	b.Publish(Event{Kind: EventNewBid, Amount: 10})
	b.Close()
}
