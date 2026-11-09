package pipeline

import (
	"context"

	"subastas-go/internal/metrics"
	"subastas-go/internal/pubsub"
)

func Run(ctx context.Context, in <-chan pubsub.Event) <-chan pubsub.Event {
	input := make(chan pubsub.Event)
	processed := make(chan pubsub.Event)
	out := make(chan pubsub.Event)

	go stageInput(ctx, in, input)
	go stageProcess(ctx, input, processed)
	go stageOutput(ctx, processed, out)

	return out
}

func stageInput(ctx context.Context, in <-chan pubsub.Event, out chan<- pubsub.Event) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-in:
			if !open {
				return
			}
			metrics.Default.PipelineReceived()
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
}

func stageProcess(ctx context.Context, in <-chan pubsub.Event, out chan<- pubsub.Event) {
	defer close(out)

	lastEvent := make(map[string]pubsub.Event)

	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-in:
			if !open {
				return
			}
			if !accept(ev, lastEvent) {
				continue
			}
			metrics.Default.PipelineProcessed()
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
}

func accept(ev pubsub.Event, lastEvent map[string]pubsub.Event) bool {
	if ev.LotID == "" {
		return false
	}

	switch ev.Kind {
	case pubsub.EventNewBid:
		if ev.Amount <= 0 {
			return false
		}
		if previous, exists := lastEvent[ev.LotID]; exists && previous == ev {
			return false
		}
		lastEvent[ev.LotID] = ev
		return true

	case pubsub.EventClosed:
		lastEvent[ev.LotID] = ev
		return true

	default:
		return false
	}
}

func stageOutput(ctx context.Context, in <-chan pubsub.Event, out chan<- pubsub.Event) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-in:
			if !open {
				return
			}
			select {
			case out <- ev:
				metrics.Default.PipelineSent()
			case <-ctx.Done():
				return
			}
		}
	}
}
