package settlement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"subastas-go/internal/domain"
)

func validInput() Input {
	return Input{
		LotID:    "reloj-lujo",
		LotName:  "Reloj de lujo",
		Start:    100,
		Duration: time.Second,
		Result:   domain.Result{Winner: "ana", Amount: 150},
		Highest:  domain.Result{Winner: "ana", Amount: 150},
	}
}

func TestValidateOK(t *testing.T) {
	defer goleak.VerifyNone(t)

	names, err := Validate(context.Background(), validInput(), 2)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if len(names) != len(checks) {
		t.Fatalf("comprobaciones = %d, se esperaban %d", len(names), len(checks))
	}
}

func TestValidateDetectsInconsistentState(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := validInput()
	in.Highest = domain.Result{Winner: "luis", Amount: 200}

	_, err := Validate(context.Background(), in, 2)
	if err == nil {
		t.Fatal("se esperaba un error de estado final")
	}
	if !strings.Contains(err.Error(), "estado-final") {
		t.Fatalf("error = %v, se esperaba que mencione estado-final", err)
	}
}

func TestValidateDetectsWinnerWithoutBids(t *testing.T) {
	defer goleak.VerifyNone(t)

	in := validInput()
	in.Result = domain.Result{Winner: "ana", Amount: in.Start}
	in.Highest = in.Result

	_, err := Validate(context.Background(), in, 2)
	if err == nil {
		t.Fatal("se esperaba un error de ganador")
	}
	if !strings.Contains(err.Error(), "ganador") {
		t.Fatalf("error = %v, se esperaba que mencione ganador", err)
	}
}

func TestValidateCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Validate(ctx, validInput(), 2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, se esperaba context.Canceled", err)
	}
}
