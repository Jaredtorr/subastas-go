package strategy

import "subastas-go/internal/domain"

type op int

const (
	opBid op = iota
	opClose
	opHighest
)

type request struct {
	op     op
	bidder string
	amount int64
	reply  chan response // capacidad 1: el actor nunca se bloquea al responder
}

type response struct {
	err error
	res domain.Result
}

// ActorAuction: estrategia C. Una sola goroutine (el actor) es dueña del
// estado; los demás le envían peticiones por canal. No hay candados.
//
// Dueño: NewActor crea la goroutine del actor.
// Terminación: al recibir opClose. Tras eso nadie más envía a reqs.
// Canal reqs: nunca se cierra; el actor es el único que lee de él.
// Canal done: lo cierra solo el actor (defer) al terminar.
// final se escribe antes de cerrar done, así que leerlo después de <-done es seguro.
type ActorAuction struct {
	reqs  chan request
	done  chan struct{}
	final domain.Result
}

func NewActor(startPrice int64) *ActorAuction {
	a := &ActorAuction{
		reqs: make(chan request),
		done: make(chan struct{}),
	}
	go a.loop(startPrice)
	return a
}

func (a *ActorAuction) loop(amount int64) {
	defer close(a.done)
	var bidder string
	for {
		r := <-a.reqs
		switch r.op {
		case opBid:
			if r.amount <= amount {
				r.reply <- response{err: domain.ErrBidTooLow}
				continue
			}
			bidder, amount = r.bidder, r.amount
			r.reply <- response{}
		case opHighest:
			r.reply <- response{res: domain.Result{Winner: bidder, Amount: amount}}
		case opClose:
			a.final = domain.Result{Winner: bidder, Amount: amount}
			r.reply <- response{res: a.final}
			return
		}
	}
}

// call envía la petición o detecta que el actor ya terminó. Si el envío se
// completa, el actor la recibió y siempre responde, así que <-r.reply no se cuelga.
func (a *ActorAuction) call(r request) (response, bool) {
	r.reply = make(chan response, 1)
	select {
	case a.reqs <- r:
		return <-r.reply, true
	case <-a.done:
		return response{}, false
	}
}

func (a *ActorAuction) Bid(bidder string, amount int64) error {
	resp, ok := a.call(request{op: opBid, bidder: bidder, amount: amount})
	if !ok {
		return domain.ErrClosed
	}
	return resp.err
}

func (a *ActorAuction) Close() domain.Result {
	resp, ok := a.call(request{op: opClose})
	if !ok {
		return a.final
	}
	return resp.res
}

func (a *ActorAuction) Highest() domain.Result {
	resp, ok := a.call(request{op: opHighest})
	if !ok {
		return a.final
	}
	return resp.res
}
