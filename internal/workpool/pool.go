package workpool

import (
	"context"
	"errors"
	"log"
	"sync"

	"subastas-go/internal/metrics"
)

var ErrClosed = errors.New("worker pool cerrado")

type Job func(context.Context) error

type task struct {
	ctx context.Context
	job Job
}

type Pool struct {
	jobs   chan task
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
	size   int
}

func New(size int) *Pool {
	if size < 1 {
		size = 1
	}

	p := &Pool{jobs: make(chan task, size), size: size}
	metrics.Default.AddPoolWorkers(size)
	p.wg.Add(size)
	for i := 0; i < size; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for task := range p.jobs {
		metrics.Default.PoolJobStarted()
		if err := task.ctx.Err(); err == nil {
			if err := task.job(task.ctx); err != nil {
				log.Printf("worker pool: trabajo falló: %v", err)
			}
		}
		metrics.Default.PoolJobFinished()
	}
}

func (p *Pool) Submit(ctx context.Context, job Job) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	metrics.Default.PoolJobQueued()
	select {
	case p.jobs <- task{ctx: ctx, job: job}:
		return nil
	case <-ctx.Done():
		metrics.Default.PoolJobCanceled()
		return ctx.Err()
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
		metrics.Default.AddPoolWorkers(-p.size)
	}
	p.mu.Unlock()

	p.wg.Wait()
}
