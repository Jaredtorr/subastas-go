package pubsub

import (
	"sync"

	"subastas-go/internal/domain"
)

type EventKind int

const (
	EventNewBid EventKind = iota
	EventClosed
)

// Event se difunde a todos los suscriptores: nueva puja máxima o cierre.
type Event struct {
	Kind   EventKind
	Bidder string
	Amount int64
}

// Subscription: el suscriptor solo lee C. C se cierra cuando el broker lo
// desconecta por lento o cuando el broker termina. El único que cierra c es
// el broker.
type Subscription struct {
	C <-chan Event
	c chan Event
}

// Broker: una goroutine (loop) es dueña del mapa de suscriptores, sin Mutex.
//
// Dueño: NewBroker crea la goroutine loop.
// Terminación: al cerrar quit (Close). Antes de salir vacía pub, cierra los
// canales de todos los suscriptores y cierra done.
// Canales: pub y sub nunca se cierran (varios envían, solo loop lee).
// quit lo cierra Close (con once); done lo cierra solo loop.
// Política de lentos: la entrega nunca bloquea; si la cola de un suscriptor
// está llena, se cierra su canal y se le da de baja.
type Broker struct {
	pub  chan Event
	sub  chan *Subscription
	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewBroker() *Broker {
	b := &Broker{
		pub:  make(chan Event, 256),
		sub:  make(chan *Subscription),
		quit: make(chan struct{}),
		done: make(chan struct{}),
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
			default: // cola llena: suscriptor lento, se desconecta
				close(s.c)
				delete(subs, s)
			}
		}
	}

	for {
		select {
		case s := <-b.sub:
			subs[s] = struct{}{}
		case ev := <-b.pub:
			deliver(ev)
		case <-b.quit:
			for { // entrega lo ya publicado antes de salir
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

// Subscribe registra un suscriptor con una cola de buf eventos.
func (b *Broker) Subscribe(buf int) *Subscription {
	c := make(chan Event, buf)
	s := &Subscription{C: c, c: c}
	select {
	case b.sub <- s:
	case <-b.done: // broker ya cerrado: devuelve un canal cerrado
		close(c)
	}
	return s
}

// Publish no se cuelga si el broker ya terminó.
func (b *Broker) Publish(ev Event) {
	select {
	case b.pub <- ev:
	case <-b.done:
	}
}

// Close detiene el broker y espera a que termine. Es idempotente.
func (b *Broker) Close() {
	b.once.Do(func() { close(b.quit) })
	<-b.done
}

// Notifying envuelve cualquier estrategia y publica un evento por cada
// puja aceptada y uno al cerrar. Se publica DESPUÉS de decidir la puja,
// nunca dentro del candado de la subasta.
type Notifying struct {
	domain.Auction
	b    *Broker
	once sync.Once
}

var _ domain.Auction = (*Notifying)(nil)

func NewNotifying(a domain.Auction, b *Broker) *Notifying {
	return &Notifying{Auction: a, b: b}
}

func (n *Notifying) Bid(bidder string, amount int64) error {
	err := n.Auction.Bid(bidder, amount)
	if err == nil {
		n.b.Publish(Event{Kind: EventNewBid, Bidder: bidder, Amount: amount})
	}
	return err
}

func (n *Notifying) Close() domain.Result {
	res := n.Auction.Close()
	n.once.Do(func() {
		n.b.Publish(Event{Kind: EventClosed, Bidder: res.Winner, Amount: res.Amount})
	})
	return res
}
