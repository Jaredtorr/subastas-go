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

	"subastas-go/internal/auction"
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

	var a auction.Auction
	switch *strategy {
	case "mutex":
		a = auction.NewMutex(*start)
	case "cas":
		a = auction.NewCAS(*start)
	case "actor":
		a = auction.NewActor(*start)
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
			auction.Bidder(ctx, a, fmt.Sprintf("postor-%d", i), int64(i), int64(*bidders))
		}()
	}

	res := <-auction.Run(ctx, a, *timeout)
	wg.Wait()

	if res.Winner == "" {
		fmt.Println("subasta cerrada sin pujas")
		return
	}
	fmt.Printf("subasta cerrada: gana %s con %d\n", res.Winner, res.Amount)
}
