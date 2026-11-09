package metrics

import "testing"

func TestSnapshotComputesPendingAndAvailable(t *testing.T) {
	m := &Metrics{}

	m.BidderStarted()
	m.BidderStarted()

	m.AddPoolWorkers(4)
	m.PoolJobQueued()
	m.PoolJobQueued()
	m.PoolJobStarted()
	m.PoolJobFinished()

	m.FanoutStarted(3)
	m.FanoutCompleted()

	m.ErrgroupCompleted()
	m.ErrgroupError()

	m.AddSemaphoreCapacity(2)
	m.SemaphoreAcquired()

	m.PipelineReceived()
	m.PipelineProcessed()
	m.PipelineSent()

	m.PubSubPublished()
	m.PubSubDelivered()

	got := m.Snapshot()

	if got.ActiveBidders != 2 {
		t.Fatalf("postores activos = %d, se esperaban 2", got.ActiveBidders)
	}
	if got.WorkPool.Workers != 4 || got.WorkPool.Active != 0 ||
		got.WorkPool.Pending != 1 || got.WorkPool.Completed != 1 {
		t.Fatalf("worker pool = %+v", got.WorkPool)
	}
	if got.FanOut.Started != 3 || got.FanOut.Completed != 1 || got.FanOut.Pending != 2 {
		t.Fatalf("fan-out = %+v", got.FanOut)
	}
	if got.Semaphore.Capacity != 2 || got.Semaphore.Occupied != 1 || got.Semaphore.Available != 1 {
		t.Fatalf("semáforo = %+v", got.Semaphore)
	}
	if got.Errgroup.Completed != 1 || got.Errgroup.Errors != 1 {
		t.Fatalf("errgroup = %+v", got.Errgroup)
	}
	if got.Pipeline.Received != 1 || got.Pipeline.Processed != 1 || got.Pipeline.Sent != 1 {
		t.Fatalf("pipeline = %+v", got.Pipeline)
	}
	if got.PubSub.Published != 1 || got.PubSub.Delivered != 1 {
		t.Fatalf("pub/sub = %+v", got.PubSub)
	}
}

func TestSnapshotClampsNegativeValues(t *testing.T) {
	m := &Metrics{}

	m.PoolJobStarted()
	m.AddSemaphoreCapacity(1)
	m.SemaphoreAcquired()
	m.SemaphoreAcquired()
	m.FanoutCompleted()

	got := m.Snapshot()

	if got.WorkPool.Pending != 0 {
		t.Fatalf("pendientes = %d, se esperaba 0", got.WorkPool.Pending)
	}
	if got.Semaphore.Available != 0 {
		t.Fatalf("disponibles = %d, se esperaba 0", got.Semaphore.Available)
	}
	if got.FanOut.Pending != 0 {
		t.Fatalf("pendientes de fan-out = %d, se esperaba 0", got.FanOut.Pending)
	}
}
