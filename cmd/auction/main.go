package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"subastas-go/internal/app"
	"subastas-go/internal/domain"
	strat "subastas-go/internal/strategy"
)

func main() {
	strategy := flag.String("strategy", "cas", "estrategia: mutex | cas | actor")
	bidders := flag.Int("bidders", 16, "número de postores")
	timeout := flag.Duration("timeout", 2*time.Second, "duración de la subasta")
	start := flag.Int64("start", 0, "precio base")
	flag.Parse()

	if *bidders < 1 {
		fmt.Fprintln(os.Stderr, "bidders debe ser >= 1")
		os.Exit(2)
	}

	var a domain.Auction
	switch *strategy {
	case "mutex":
		a = strat.NewMutex(*start)
	case "cas":
		a = strat.NewCAS(*start)
	case "actor":
		a = strat.NewActor(*start)
	default:
		fmt.Fprintf(os.Stderr, "estrategia desconocida: %q\n", *strategy)
		os.Exit(2)
	}

	// Ctrl+C o SIGTERM cancelan ctx; de ahí se propaga a postores y subasta.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Cada postor termina con ctx.Done() o ErrClosed; main espera con wg.
	var wg sync.WaitGroup
	for i := 0; i < *bidders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app.Bidder(ctx, a, fmt.Sprintf("postor-%d", i), int64(i), int64(*bidders))
		}()
	}

	auditCtx, stopAudit := context.WithCancel(ctx)
	audit := app.Audit(auditCtx, a, *start, 10*time.Millisecond)

	res := <-app.Run(ctx, a, *timeout)
	wg.Wait()
	stopAudit()

	if err := <-audit; err != nil {
		fmt.Fprintln(os.Stderr, "VIOLACIÓN DE INVARIANTE:", err)
		os.Exit(1)
	}
	if a.Highest() != res {
		fmt.Fprintln(os.Stderr, "VIOLACIÓN: el estado cambió tras el cierre")
		os.Exit(1)
	}
	fmt.Println("auditoría: sin violaciones")

	if res.Winner == "" {
		fmt.Println("subasta cerrada sin pujas")
		return
	}
	fmt.Printf("subasta cerrada: gana %s con %d\n", res.Winner, res.Amount)
}
