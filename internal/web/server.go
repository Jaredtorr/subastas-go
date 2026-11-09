package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"subastas-go/internal/app"
	"subastas-go/internal/domain"
	"subastas-go/internal/metrics"
	"subastas-go/internal/pipeline"
	"subastas-go/internal/pubsub"
)

//go:embed index.html styles.css app.js
var assets embed.FS

var catalog = []domain.Lot{
	{
		ID:          "carro-deportivo",
		Name:        "Automóvil deportivo",
		Category:    "Vehículos",
		Description: "Automóvil deportivo de colección",
		Image:       "/images/car.jpg",
	},
	{
		ID:          "reloj-lujo",
		Name:        "Reloj de lujo",
		Category:    "Relojería",
		Description: "Reloj de alta gama",
		Image:       "/images/watch.jpg",
	},
	{
		ID:          "anillo-diamantes",
		Name:        "Anillo de diamantes",
		Category:    "Joyería",
		Description: "Anillo de diamantes engastados",
		Image:       "/images/ring.jpg",
	},
	{
		ID:          "collar-oro",
		Name:        "Collar de oro y diamantes",
		Category:    "Joyería",
		Description: "Collar de oro con diamantes",
		Image:       "/images/necklace.jpg",
	},
	{
		ID:          "pulsera-diamantes",
		Name:        "Pulsera de diamantes",
		Category:    "Joyería",
		Description: "Pulsera de diamantes",
		Image:       "/images/bracelet.jpg",
	},
	{
		ID:          "aretes-diamantes",
		Name:        "Aretes de diamantes",
		Category:    "Joyería",
		Description: "Aretes con engaste de diamantes",
		Image:       "/images/earrings.jpg",
	},
	{
		ID:          "brazalete-oro",
		Name:        "Brazalete de oro",
		Category:    "Joyería",
		Description: "Brazalete de oro",
		Image:       "/images/gold-bracelet.jpg",
	},
	{
		ID:          "collar-perlas",
		Name:        "Collar de perlas",
		Category:    "Joyería",
		Description: "Collar de perlas de colección",
		Image:       "/images/pearls.jpg",
	},
	{
		ID:          "dije-oro",
		Name:        "Dije de oro",
		Category:    "Joyería",
		Description: "Dije de oro de colección",
		Image:       "/images/pendant.jpg",
	},
	{
		ID:          "anillo-esmeralda",
		Name:        "Anillo de esmeralda",
		Category:    "Joyería",
		Description: "Anillo con piedra esmeralda",
		Image:       "/images/emerald.jpg",
	},
}

const (
	sseFlushEvery      = 100 * time.Millisecond
	sseMaxBidsPerFlush = 5
)

func NewHandler(
	broker *pubsub.Broker,
	strategy string,
	manager *app.AuctionManager,
	ctx context.Context,
) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.FS(assets)))

	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"strategy": strategy,
		})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(metrics.Default.Snapshot())
	})

	mux.HandleFunc("/catalog", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(catalog)
	})

	mux.HandleFunc("/auction/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		var request struct {
			LotID string `json:"lotId"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "Solicitud JSON inválida", http.StatusBadRequest)
			return
		}

		var selected *domain.Lot
		for i := range catalog {
			if catalog[i].ID == request.LotID {
				selected = &catalog[i]
				break
			}
		}

		if selected == nil {
			http.Error(w, "Producto no encontrado", http.StatusNotFound)
			return
		}

		if err := manager.Start(ctx, *selected); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Subasta iniciada",
			"lotId":   selected.ID,
			"lotName": selected.Name,
		})
	})

	mux.HandleFunc("/auction/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		lotID := r.URL.Query().Get("lotId")
		if lotID == "" {
			http.Error(w, "Falta el parámetro lotId", http.StatusBadRequest)
			return
		}

		snapshot, exists := manager.Get(lotID)
		if !exists {
			http.Error(w, "Subasta no encontrada", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snapshot)
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "SSE no soportado", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		sub := broker.Subscribe(1024)
		defer broker.Unsubscribe(sub)

		events := pipeline.Run(r.Context(), sub.C)

		flusher.Flush()

		pending := make(map[string][]pubsub.Event)
		sentMax := make(map[string]int64)

		var recv, sent, maxQueue, ticks int

		write := func(ev pubsub.Event) error {
			data, err := json.Marshal(ev)
			if err != nil {
				return nil
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return err
			}
			sent++
			return nil
		}

		flushLot := func(lotID string) error {
			bids := pending[lotID]
			if len(bids) == 0 {
				return nil
			}
			delete(pending, lotID)

			sort.Slice(bids, func(i, j int) bool {
				return bids[i].Amount < bids[j].Amount
			})
			if len(bids) > sseMaxBidsPerFlush {
				bids = bids[len(bids)-sseMaxBidsPerFlush:]
			}

			for _, bid := range bids {
				if bid.Amount <= sentMax[lotID] {
					continue
				}
				sentMax[lotID] = bid.Amount
				if err := write(bid); err != nil {
					return err
				}
			}
			return nil
		}

		tick := time.NewTicker(sseFlushEvery)
		defer tick.Stop()

		for {
			select {
			case <-r.Context().Done():
				return

			case <-tick.C:
				for lotID := range pending {
					if err := flushLot(lotID); err != nil {
						return
					}
				}
				flusher.Flush()

				ticks++
				if ticks%10 == 0 {
					if recv > 0 || sent > 0 {
						log.Printf("[SSE] recibidos=%d enviados=%d cola_max=%d/%d",
							recv, sent, maxQueue, cap(sub.C))
					}
					recv, sent, maxQueue = 0, 0, 0
				}

			case ev, open := <-events:
				if !open {
					return
				}

				recv++
				if q := len(sub.C); q > maxQueue {
					maxQueue = q
				}

				switch ev.Kind {
				case pubsub.EventNewBid:
					pending[ev.LotID] = append(pending[ev.LotID], ev)

				case pubsub.EventClosed:
					if err := flushLot(ev.LotID); err != nil {
						return
					}
					sentMax[ev.LotID] = ev.Amount

					if err := write(ev); err != nil {
						return
					}
					flusher.Flush()

					log.Printf("[SSE] CIERRE enviado lot=%s ganador=%s monto=%d",
						ev.LotID, ev.Bidder, ev.Amount)
				}
			}
		}
	})

	return mux
}
