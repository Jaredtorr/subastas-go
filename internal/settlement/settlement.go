package settlement

import (
	"context"
	"fmt"
	"time"

	"subastas-go/internal/domain"
	"subastas-go/internal/fanout"
)

type Input struct {
	LotID    string
	LotName  string
	Start    int64
	Duration time.Duration
	Result   domain.Result
	Highest  domain.Result
}

type check struct {
	name string
	run  func(context.Context, Input) error
}

var checks = []check{
	{name: "precio-base", run: checkPriceFloor},
	{name: "ganador", run: checkWinner},
	{name: "estado-final", run: checkFinalState},
	{name: "producto", run: checkLot},
}

func Validate(ctx context.Context, in Input, concurrency int) ([]string, error) {
	names, err := fanout.Run(ctx, checks, concurrency, func(ctx context.Context, c check) (string, error) {
		if err := c.run(ctx, in); err != nil {
			return "", fmt.Errorf("%s: %w", c.name, err)
		}
		return c.name, nil
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

func checkPriceFloor(_ context.Context, in Input) error {
	if in.Result.Amount < in.Start {
		return fmt.Errorf("el monto final %d es menor que el precio base %d", in.Result.Amount, in.Start)
	}
	return nil
}

func checkWinner(_ context.Context, in Input) error {
	if in.Result.Amount > in.Start && in.Result.Winner == "" {
		return fmt.Errorf("monto %d sin ganador", in.Result.Amount)
	}
	if in.Result.Amount == in.Start && in.Result.Winner != "" {
		return fmt.Errorf("hay ganador %q sin pujas por encima del precio base", in.Result.Winner)
	}
	return nil
}

func checkFinalState(_ context.Context, in Input) error {
	if in.Result != in.Highest {
		return fmt.Errorf("el resultado del cierre %+v no coincide con el estado final %+v", in.Result, in.Highest)
	}
	return nil
}

func checkLot(_ context.Context, in Input) error {
	if in.LotID == "" {
		return fmt.Errorf("el producto no tiene identificador")
	}
	if in.LotName == "" {
		return fmt.Errorf("el producto %q no tiene nombre", in.LotID)
	}
	if in.Duration <= 0 {
		return fmt.Errorf("la duración de la subasta debe ser positiva")
	}
	return nil
}
