package fanout

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"subastas-go/internal/metrics"
	"subastas-go/internal/semaphore"
)

type Result[R any] struct {
	Index int
	Value R
	Err   error
}

func Start[T, R any](
	ctx context.Context,
	items []T,
	limit int,
	fn func(context.Context, T) (R, error),
) <-chan Result[R] {
	out := make(chan Result[R], len(items))
	sem := semaphore.New(limit)
	metrics.Default.FanoutStarted(len(items))

	group, groupCtx := errgroup.WithContext(ctx)

	for i, item := range items {
		i, item := i, item
		group.Go(func() error {
			if err := sem.Acquire(groupCtx); err != nil {
				metrics.Default.FanoutCompleted()
				metrics.Default.ErrgroupCompleted()
				return err
			}
			defer sem.Release()

			value, err := fn(groupCtx, item)
			out <- Result[R]{Index: i, Value: value, Err: err}

			metrics.Default.FanoutCompleted()
			metrics.Default.ErrgroupCompleted()
			if isValidationError(err) {
				metrics.Default.ErrgroupError()
			}
			return err
		})
	}

	go func() {
		_ = group.Wait()
		sem.Close()
		close(out)
	}()

	return out
}

func isValidationError(err error) bool {
	return err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}

func Run[T, R any](
	ctx context.Context,
	items []T,
	limit int,
	fn func(context.Context, T) (R, error),
) ([]R, error) {
	values := make([]R, len(items))
	seen := make([]bool, len(items))

	var firstErr error
	for result := range Start(ctx, items, limit, fn) {
		if result.Err != nil {
			if firstErr == nil || (errors.Is(firstErr, context.Canceled) && !errors.Is(result.Err, context.Canceled)) {
				firstErr = result.Err
			}
			continue
		}
		values[result.Index] = result.Value
		seen[result.Index] = true
	}

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i, ok := range seen {
		if !ok {
			return nil, fmt.Errorf("fanout: no se recibió el resultado %d", i)
		}
	}

	return values, nil
}
