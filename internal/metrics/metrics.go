package metrics

import (
	"runtime"
	"sync/atomic"
)

type Metrics struct {
	activeBidders atomic.Int64

	poolWorkers   atomic.Int64
	poolActive    atomic.Int64
	poolPending   atomic.Int64
	poolCompleted atomic.Int64

	fanoutStarted   atomic.Int64
	fanoutCompleted atomic.Int64

	semaphoreCapacity    atomic.Int64
	semaphoreMaxCapacity atomic.Int64
	semaphoreOccupied    atomic.Int64

	errgroupCompleted atomic.Int64
	errgroupErrors    atomic.Int64

	pipelineReceived  atomic.Int64
	pipelineProcessed atomic.Int64
	pipelineSent      atomic.Int64

	pubsubPublished atomic.Int64
	pubsubDelivered atomic.Int64
}

var Default = &Metrics{}

type Snapshot struct {
	Goroutines    int               `json:"goroutines"`
	ActiveBidders int64             `json:"activeBidders"`
	WorkPool      WorkPoolSnapshot  `json:"workPool"`
	FanOut        FanOutSnapshot    `json:"fanOut"`
	Semaphore     SemaphoreSnapshot `json:"semaphore"`
	Errgroup      ErrgroupSnapshot  `json:"errgroup"`
	Pipeline      PipelineSnapshot  `json:"pipeline"`
	PubSub        PubSubSnapshot    `json:"pubSub"`
}

type WorkPoolSnapshot struct {
	Workers   int64 `json:"workers"`
	Active    int64 `json:"active"`
	Pending   int64 `json:"pending"`
	Completed int64 `json:"completed"`
}

type FanOutSnapshot struct {
	Started   int64 `json:"started"`
	Completed int64 `json:"completed"`
	Pending   int64 `json:"pending"`
}

type SemaphoreSnapshot struct {
	Capacity    int64 `json:"capacity"`
	MaxCapacity int64 `json:"maxCapacity"`
	Occupied    int64 `json:"occupied"`
	Available   int64 `json:"available"`
}

type ErrgroupSnapshot struct {
	Completed int64 `json:"completed"`
	Errors    int64 `json:"errors"`
}

type PipelineSnapshot struct {
	Received  int64 `json:"received"`
	Processed int64 `json:"processed"`
	Sent      int64 `json:"sent"`
}

type PubSubSnapshot struct {
	Published int64 `json:"published"`
	Delivered int64 `json:"delivered"`
}

func (m *Metrics) Snapshot() Snapshot {
	fanoutStarted := m.fanoutStarted.Load()
	fanoutCompleted := m.fanoutCompleted.Load()

	capacity := m.semaphoreCapacity.Load()
	occupied := m.semaphoreOccupied.Load()

	return Snapshot{
		Goroutines:    runtime.NumGoroutine(),
		ActiveBidders: m.activeBidders.Load(),
		WorkPool: WorkPoolSnapshot{
			Workers:   m.poolWorkers.Load(),
			Active:    m.poolActive.Load(),
			Pending:   nonNegative(m.poolPending.Load()),
			Completed: m.poolCompleted.Load(),
		},
		FanOut: FanOutSnapshot{
			Started:   fanoutStarted,
			Completed: fanoutCompleted,
			Pending:   nonNegative(fanoutStarted - fanoutCompleted),
		},
		Semaphore: SemaphoreSnapshot{
			Capacity:    capacity,
			MaxCapacity: m.semaphoreMaxCapacity.Load(),
			Occupied:    occupied,
			Available:   nonNegative(capacity - occupied),
		},
		Errgroup: ErrgroupSnapshot{
			Completed: m.errgroupCompleted.Load(),
			Errors:    m.errgroupErrors.Load(),
		},
		Pipeline: PipelineSnapshot{
			Received:  m.pipelineReceived.Load(),
			Processed: m.pipelineProcessed.Load(),
			Sent:      m.pipelineSent.Load(),
		},
		PubSub: PubSubSnapshot{
			Published: m.pubsubPublished.Load(),
			Delivered: m.pubsubDelivered.Load(),
		},
	}
}

func (m *Metrics) BidderStarted() {
	m.activeBidders.Add(1)
}

func (m *Metrics) BidderFinished() {
	m.activeBidders.Add(-1)
}

func (m *Metrics) AddPoolWorkers(delta int) {
	m.poolWorkers.Add(int64(delta))
}

func (m *Metrics) PoolJobQueued() {
	m.poolPending.Add(1)
}

func (m *Metrics) PoolJobCanceled() {
	m.poolPending.Add(-1)
}

func (m *Metrics) PoolJobStarted() {
	m.poolPending.Add(-1)
	m.poolActive.Add(1)
}

func (m *Metrics) PoolJobFinished() {
	m.poolActive.Add(-1)
	m.poolCompleted.Add(1)
}

func (m *Metrics) FanoutStarted(total int) {
	m.fanoutStarted.Add(int64(total))
}

func (m *Metrics) FanoutCompleted() {
	m.fanoutCompleted.Add(1)
}

func (m *Metrics) AddSemaphoreCapacity(delta int) {
	capacity := m.semaphoreCapacity.Add(int64(delta))

	if delta > 0 {
		for {
			maximum := m.semaphoreMaxCapacity.Load()

			if capacity <= maximum {
				break
			}

			if m.semaphoreMaxCapacity.CompareAndSwap(maximum, capacity) {
				break
			}
		}
	}
}

func (m *Metrics) SemaphoreAcquired() {
	m.semaphoreOccupied.Add(1)
}

func (m *Metrics) SemaphoreReleased() {
	m.semaphoreOccupied.Add(-1)
}

func (m *Metrics) ErrgroupCompleted() {
	m.errgroupCompleted.Add(1)
}

func (m *Metrics) ErrgroupError() {
	m.errgroupErrors.Add(1)
}

func (m *Metrics) PipelineReceived() {
	m.pipelineReceived.Add(1)
}

func (m *Metrics) PipelineProcessed() {
	m.pipelineProcessed.Add(1)
}

func (m *Metrics) PipelineSent() {
	m.pipelineSent.Add(1)
}

func (m *Metrics) PubSubPublished() {
	m.pubsubPublished.Add(1)
}

func (m *Metrics) PubSubDelivered() {
	m.pubsubDelivered.Add(1)
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
