package app

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"

	"subastas-go/internal/domain"
	"subastas-go/internal/pubsub"
)

func TestManagerRunsAuctionAndSettlement(t *testing.T) {
	defer goleak.VerifyNone(t)

	broker := pubsub.NewBroker()
	manager, err := NewAuctionManager(broker, "cas", 8, 0, 50*time.Millisecond, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		manager.Close()
		broker.Close()
	}()

	lot := domain.Lot{ID: "lote-prueba", Name: "Lote de prueba"}
	if err := manager.Start(context.Background(), lot); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, exists := manager.Get(lot.ID)
		if exists && snapshot.Closed {
			if snapshot.Result.Winner == "" || snapshot.Result.Amount == 0 {
				t.Fatalf("resultado incompleto: %+v", snapshot.Result)
			}
			if snapshot.Highest != snapshot.Result {
				t.Fatalf("estado final %+v no coincide con el resultado %+v", snapshot.Highest, snapshot.Result)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("la subasta no cerró a tiempo")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
