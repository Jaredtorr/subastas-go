package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"subastas-go/internal/domain"
	"subastas-go/internal/pubsub"
	"subastas-go/internal/settlement"
	strat "subastas-go/internal/strategy"
	"subastas-go/internal/workpool"
)

var (
	ErrLotNotFound    = errors.New("producto no encontrado")
	ErrAlreadyRunning = errors.New("el producto ya tiene una subasta activa")
)

type AuctionManager struct {
	mu        sync.RWMutex
	auctions  map[string]*managedAuction
	broker    *pubsub.Broker
	strategy  string
	bidders   int
	start     int64
	duration  time.Duration
	workers   int
	pool      *workpool.Pool
	runWG     sync.WaitGroup
	closeOnce sync.Once
}

type managedAuction struct {
	lot     domain.Lot
	auction domain.Auction
	cancel  context.CancelFunc
	result  domain.Result
	closed  bool
}

type AuctionSnapshot struct {
	LotID   string        `json:"lotId"`
	LotName string        `json:"lotName"`
	Active  bool          `json:"active"`
	Closed  bool          `json:"closed"`
	Result  domain.Result `json:"result"`
	Highest domain.Result `json:"highest"`
}

func NewAuctionManager(
	broker *pubsub.Broker,
	strategy string,
	bidders int,
	start int64,
	duration time.Duration,
	workers int,
) (*AuctionManager, error) {
	if broker == nil {
		return nil, errors.New("broker requerido")
	}
	if bidders < 1 {
		return nil, errors.New("bidders debe ser >= 1")
	}
	if duration <= 0 {
		return nil, errors.New("duración debe ser mayor a cero")
	}
	if workers < 1 {
		workers = 1
	}
	switch strategy {
	case "mutex", "cas", "actor":
	default:
		return nil, fmt.Errorf("estrategia desconocida: %s", strategy)
	}

	return &AuctionManager{
		auctions: make(map[string]*managedAuction),
		broker:   broker,
		strategy: strategy,
		bidders:  bidders,
		start:    start,
		duration: duration,
		workers:  workers,
		pool:     workpool.New(workers),
	}, nil
}

func (m *AuctionManager) newAuction() domain.Auction {
	switch m.strategy {
	case "mutex":
		return strat.NewMutex(m.start)
	case "cas":
		return strat.NewCAS(m.start)
	default:
		return strat.NewActor(m.start)
	}
}

func (m *AuctionManager) Start(parent context.Context, lot domain.Lot) error {
	m.mu.Lock()

	if current, exists := m.auctions[lot.ID]; exists && !current.closed {
		m.mu.Unlock()
		return ErrAlreadyRunning
	}

	ctx, cancel := context.WithCancel(parent)
	auction := m.newAuction()
	notifying := pubsub.NewNotifyingForLot(auction, m.broker, lot.ID, lot.Name)

	entry := &managedAuction{
		lot:     lot,
		auction: notifying,
		cancel:  cancel,
	}
	m.auctions[lot.ID] = entry
	m.mu.Unlock()

	m.runWG.Add(1)
	go m.run(ctx, lot, entry)
	return nil
}

func (m *AuctionManager) run(ctx context.Context, lot domain.Lot, entry *managedAuction) {
	defer m.runWG.Done()

	var wg sync.WaitGroup

	for i := 0; i < m.bidders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			Bidder(
				ctx,
				entry.auction,
				fmt.Sprintf("postor-%d", id),
				int64(id),
				int64(m.bidders),
			)
		}(i)
	}

	auditCtx, stopAudit := context.WithCancel(ctx)
	audit := Audit(auditCtx, entry.auction, m.start, 10*time.Millisecond)

	result := <-Run(ctx, entry.auction, m.duration)

	wg.Wait()
	stopAudit()

	if err := <-audit; err != nil {
		fmt.Printf("auditoría de %s: %v\n", lot.Name, err)
	} else {
		fmt.Printf("auditoría de %s: sin violaciones\n", lot.Name)
	}

	entry.cancel()

	m.mu.Lock()
	entry.result = result
	entry.closed = true
	m.mu.Unlock()

	fmt.Printf("subasta cerrada: %s; ganador: %s; monto: %d\n",
		lot.Name, result.Winner, result.Amount)

	m.enqueueSettlement(lot, entry, result)
}

func (m *AuctionManager) enqueueSettlement(lot domain.Lot, entry *managedAuction, result domain.Result) {
	err := m.pool.Submit(context.Background(), func(ctx context.Context) error {
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		names, err := settlement.Validate(checkCtx, settlement.Input{
			LotID:    lot.ID,
			LotName:  lot.Name,
			Start:    m.start,
			Duration: m.duration,
			Result:   result,
			Highest:  entry.auction.Highest(),
		}, m.workers)
		if err != nil {
			return fmt.Errorf("liquidación de %s: %w", lot.Name, err)
		}

		fmt.Printf("liquidación de %s: %d comprobaciones correctas\n", lot.Name, len(names))
		return nil
	})
	if err != nil {
		fmt.Printf("no se pudo encolar la liquidación de %s: %v\n", lot.Name, err)
	}
}

func (m *AuctionManager) Close() {
	m.closeOnce.Do(func() {
		m.runWG.Wait()
		m.pool.Close()
	})
}

func (m *AuctionManager) Get(lotID string) (AuctionSnapshot, bool) {
	m.mu.RLock()
	entry, exists := m.auctions[lotID]
	if !exists {
		m.mu.RUnlock()
		return AuctionSnapshot{}, false
	}

	snapshot := AuctionSnapshot{
		LotID:   lotID,
		LotName: entry.lot.Name,
		Active:  !entry.closed,
		Closed:  entry.closed,
		Result:  entry.result,
	}

	auction := entry.auction
	m.mu.RUnlock()

	snapshot.Highest = auction.Highest()
	return snapshot, true
}
