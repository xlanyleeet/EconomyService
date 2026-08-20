package http

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"economy-service/internal/domain"
	"economy-service/internal/event/pubsub"
	"economy-service/internal/repository/postgres"
	"economy-service/internal/repository/redis"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httprate"
)

type APIServer struct {
	db          *postgres.Database
	cacheRepo   *redis.CacheRepository
	leaderboard *redis.LeaderboardRepository
	publisher   *pubsub.Publisher
	port        string
	apiKey      string
}

func NewAPIServer(
	db *postgres.Database,
	cacheRepo *redis.CacheRepository,
	leaderboard *redis.LeaderboardRepository,
	publisher *pubsub.Publisher,
	port string,
	apiKey string,
) *APIServer {
	return &APIServer{
		db:          db,
		cacheRepo:   cacheRepo,
		leaderboard: leaderboard,
		publisher:   publisher,
		port:        port,
		apiKey:      apiKey,
	}
}

func (s *APIServer) Start(ctx context.Context) {
	r := chi.NewRouter()

	// Built-in Chi Middlewares
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(5 * time.Second))

	// Rate limiting: 100 requests per minute per IP
	r.Use(httprate.LimitByIP(100, 1*time.Minute))

	// Routes
	r.Get("/health", s.handleHealth)

	r.Route("/api/v1/economy", func(r chi.Router) {
		r.Get("/balance", s.handleGetBalance)
		r.Get("/balance/{uuid}", s.handleGetBalance)

		r.Get("/leaderboard", s.handleLeaderboard)

		// Protected endpoints
		r.With(s.authMiddleware).Post("/transaction", s.handleTransaction)
		r.With(s.authMiddleware).Post("/daily-bonus/claim", s.handleClaimDailyBonus)
		r.With(s.authMiddleware).Post("/daily-bonus/claim/{uuid}", s.handleClaimDailyBonus)
	})

	server := &http.Server{
		Addr:         s.port,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting Economy REST API server (Chi) on %s", s.port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("API server failed: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func writeJSON(w http.ResponseWriter, status int, resp domain.APIResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}
