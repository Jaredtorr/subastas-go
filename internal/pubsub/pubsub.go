package pubsub

import (
	"sync"

	"subastas-go/internal/domain"
	"subastas-go/internal/metrics"
)

type EventKind int

const (
	EventNewBid EventKind = iota
	EventClosed
)

type Event struct {
	Kind    EventKind
	Bidder  string
	Amount  int64
	LotID   string
	LotName string
}

type Subscription struct {
	C <-chan Event
	c chan Event
}

type Broker struct {
	pub   chan Event
	sub   chan *Subscription
	unsub chan *Subscription
	quit  chan struct{}
	done  chan struct{}
	once  sync.Once
}

func NewBroker() *Broker {
	b := &Broker{
		pub:   make(chan Event, 256),
		sub:   make(chan *Subscription),
		unsub: make(chan *Subscription),
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}

	go b.loop()
	return b
}

func (b *Broker) loop() {
	subs := make(map[*Subscription]struct{})

	defer func() {
		for s := range subs {
			close(s.c)
		}
		close(b.done)
	}()

	deliver := func(ev Event) {
		for s := range subs {
			select {
			case s.c <- ev:
				metrics.Default.PubSubDelivered()

			default:
				if ev.Kind == EventClosed {
					select {
					case <-s.c:
					default:
					}

					select {
					case s.c <- ev:
						metrics.Default.PubSubDelivered()
					default:
					}
				}
			}
		}
	}

	for {
		select {
		case s := <-b.sub:
			subs[s] = struct{}{}

		case s := <-b.unsub:
			if _, exists := subs[s]; exists {
				delete(subs, s)
				close(s.c)
			}

		case ev := <-b.pub:
			deliver(ev)

		case <-b.quit:
			for {
				select {
				case ev := <-b.pub:
					deliver(ev)
				default:
					return
				}
			}
		}
	}
}

func (b *Broker) Subscribe(buf int) *Subscription {
	if buf < 1 {
		buf = 1
	}

	c := make(chan Event, buf)
	s := &Subscription{C: c, c: c}

	select {
	case b.sub <- s:
	case <-b.done:
		close(c)
	}

	return s
}

func (b *Broker) Unsubscribe(s *Subscription) {
	if s == nil {
		return
	}

	select {
	case b.unsub <- s:
	case <-b.done:
	}
}

func (b *Broker) Publish(ev Event) {
	select {
	case b.pub <- ev:
		metrics.Default.PubSubPublished()
	case <-b.done:
	}
}

func (b *Broker) Close() {
	b.once.Do(func() {
		close(b.quit)
	})
	<-b.done
}

type Notifying struct {
	domain.Auction
	b       *Broker
	lotID   string
	lotName string
	once    sync.Once
}

var _ domain.Auction = (*Notifying)(nil)

func NewNotifying(a domain.Auction, b *Broker) *Notifying {
	return NewNotifyingForLot(a, b, "", "")
}

func NewNotifyingForLot(
	a domain.Auction,
	b *Broker,
	lotID string,
	lotName string,
) *Notifying {
	return &Notifying{
		Auction: a,
		b:       b,
		lotID:   lotID,
		lotName: lotName,
	}
}

func (n *Notifying) Bid(bidder string, amount int64) error {
	err := n.Auction.Bid(bidder, amount)

	if err == nil {
		n.b.Publish(Event{
			Kind:    EventNewBid,
			Bidder:  bidder,
			Amount:  amount,
			LotID:   n.lotID,
			LotName: n.lotName,
		})
	}

	return err
}

func (n *Notifying) Close() domain.Result {
	res := n.Auction.Close()

	n.once.Do(func() {
		n.b.Publish(Event{
			Kind:    EventClosed,
			Bidder:  res.Winner,
			Amount:  res.Amount,
			LotID:   n.lotID,
			LotName: n.lotName,
		})
	})

	return res
}
