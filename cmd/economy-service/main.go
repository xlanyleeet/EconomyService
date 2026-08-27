package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"economy-service/internal/config"
	"economy-service/internal/event/pubsub"
	"economy-service/internal/event/stream"
	httphandler "economy-service/internal/handler/http"
	"economy-service/internal/repository/postgres"
	"economy-service/internal/repository/redis"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log.Println("===========================================")
	log.Println(" Starting Minigames EconomyService (Go)  ")
	log.Println("===========================================")

	cfg := config.LoadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Database
	db, err := postgres.NewDatabase(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()
	log.Println("Successfully connected to PostgreSQL")

	// Initialize Worker ID
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	log.Printf("EconomyService Worker ID initialized: %s", workerID)

	var wg sync.WaitGroup

	// Initialize Redis Client & Repositories
	redisClient := redis.NewClient(cfg.RedisAddr, cfg.RedisPassword)
	defer redisClient.Close()
	log.Println("Successfully connected to Redis")

	cacheRepo := redis.NewCacheRepository(redisClient)
	leaderboardRepo := redis.NewLeaderboardRepository(redisClient)
	publisher := pubsub.NewPublisher(redisClient)

	// Pre-sync Redis Leaderboard from Postgres
	if err := leaderboardRepo.SyncLeaderboardFromDB(ctx, db); err != nil {
		log.Printf("Warning: Failed to sync initial leaderboard from DB: %v", err)
	}

	// Initialize REST API Server
	apiServer := httphandler.NewAPIServer(db, cacheRepo, leaderboardRepo, publisher, cfg.APIPort, cfg.APIKey)
	apiServer.Start(ctx)

	// Initialize Prometheus metrics server with explicit timeouts
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:         cfg.MetricsPort,
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting Prometheus metrics on %s", cfg.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	// Start Pub/Sub Listeners
	sub := pubsub.NewSubscriber(redisClient, db, cacheRepo, leaderboardRepo, publisher, &wg)
	sub.StartAllListeners(ctx)

	// Start Redis Stream Match Consumer
	matchConsumer := stream.NewMatchConsumer(redisClient, db, cacheRepo, leaderboardRepo, publisher, workerID, &wg)
	wg.Add(1)
	go matchConsumer.StartListening(ctx)

	log.Println("EconomyService is fully operational and waiting for match results & level up events...")

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutdown signal received, closing EconomyService gracefully...")
	cancel()
	wg.Wait()
	log.Println("EconomyService shutdown complete.")
}
