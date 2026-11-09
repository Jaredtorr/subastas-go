package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"subastas-go/internal/app"
	"subastas-go/internal/pubsub"
	"subastas-go/internal/web"
)

func main() {
	strategy := flag.String("strategy", "cas", "estrategia: mutex | cas | actor")
	bidders := flag.Int("bidders", 16, "número de postores")
	timeout := flag.Duration("timeout", 30*time.Second, "duración de cada subasta")
	start := flag.Int64("start", 0, "precio base")
	workers := flag.Int("workers", 4, "trabajadores del pool de liquidación y comprobaciones concurrentes")
	httpAddr := flag.String("http", ":8080", "dirección del servidor web; vacío para desactivarlo")
	flag.Parse()

	if *bidders < 1 {
		fmt.Fprintln(os.Stderr, "bidders debe ser >= 1")
		os.Exit(2)
	}
	if *workers < 1 {
		fmt.Fprintln(os.Stderr, "workers debe ser >= 1")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	broker := pubsub.NewBroker()
	defer broker.Close()

	manager, err := app.NewAuctionManager(
		broker,
		*strategy,
		*bidders,
		*start,
		*timeout,
		*workers,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error al crear administrador:", err)
		os.Exit(2)
	}
	defer manager.Close()

	if *httpAddr == "" {
		fmt.Println("Servidor web desactivado.")
		return
	}

	listener, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "no se pudo iniciar el servidor:", err)
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:    *httpAddr,
		Handler: web.NewHandler(broker, *strategy, manager, ctx),
	}

	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Fprintln(os.Stderr, "error del servidor web:", err)
			stop()
		}
	}()

	fmt.Printf("interfaz web: http://localhost%s\n", *httpAddr)
	fmt.Printf("estrategia: %s\n", *strategy)
	fmt.Printf("postores por subasta: %d\n", *bidders)
	fmt.Printf("trabajadores de liquidación: %d\n", *workers)
	fmt.Printf("duración por subasta: %s\n", *timeout)
	fmt.Println("Selecciona un producto para iniciar su subasta.")
	fmt.Println("Presiona Ctrl+C para salir.")

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "error al cerrar servidor:", err)
	}
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintln(os.Stderr, "error al completar cierre:", err)
	}

	manager.Close()
	broker.Close()
}
