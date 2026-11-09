package pipeline

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"

	"subastas-go/internal/pubsub"
)

func collect(t *testing.T, out <-chan pubsub.Event) []pubsub.Event {
	t.Helper()
	var events []pubsub.Event
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case ev, open := <-out:
			if !open {
				return events
			}
			events = append(events, ev)
		case <-timeout.C:
			t.Fatal("el pipeline no cerró su canal de salida")
		}
	}
}

func TestPipelineProcessesAndFilters(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := make(chan pubsub.Event)
	out := Run(context.Background(), in)

	input := []pubsub.Event{
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "ana", Amount: 10},
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "ana", Amount: 10},
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "luis", Amount: 5},
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "luis", Amount: 20},
		{Kind: pubsub.EventNewBid, LotID: "", Bidder: "sin-lote", Amount: 30},
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "cero", Amount: 0},
		{Kind: pubsub.EventClosed, LotID: "reloj", Bidder: "luis", Amount: 20},
	}
	go func() {
		defer close(in)
		for _, ev := range input {
			in <- ev
		}
	}()

	got := collect(t, out)
	want := []pubsub.Event{
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "ana", Amount: 10},
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "luis", Amount: 5},
		{Kind: pubsub.EventNewBid, LotID: "reloj", Bidder: "luis", Amount: 20},
		{Kind: pubsub.EventClosed, LotID: "reloj", Bidder: "luis", Amount: 20},
	}

	if len(got) != len(want) {
		t.Fatalf("eventos = %+v, se esperaban %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("evento %d = %+v, se esperaba %+v", i, got[i], want[i])
		}
	}
}

func TestPipelineKeepsCloseEventWithoutBids(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := make(chan pubsub.Event)
	out := Run(context.Background(), in)

	closeEvent := pubsub.Event{Kind: pubsub.EventClosed, LotID: "anillo", Bidder: "", Amount: 0}
	in <- closeEvent
	close(in)

	got := collect(t, out)
	if len(got) != 1 || got[0] != closeEvent {
		t.Fatalf("eventos = %+v, se esperaba solo el cierre", got)
	}
}

func TestPipelineClosesOutputWhenInputCloses(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := make(chan pubsub.Event)
	out := Run(context.Background(), in)
	close(in)

	if got := collect(t, out); len(got) != 0 {
		t.Fatalf("eventos = %+v, se esperaba ninguno", got)
	}
}

func TestPipelineCancelStopsAllStages(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan pubsub.Event)
	out := Run(ctx, in)

	in <- pubsub.Event{Kind: pubsub.EventNewBid, LotID: "collar", Bidder: "ana", Amount: 1}
	if ev := <-out; ev.Amount != 1 {
		t.Fatalf("evento inesperado: %+v", ev)
	}

	cancel()

	select {
	case _, open := <-out:
		if open {
			t.Fatal("el canal de salida debería cerrarse tras cancelar")
		}
	case <-time.After(time.Second):
		t.Fatal("el pipeline no se detuvo tras cancelar")
	}
}
