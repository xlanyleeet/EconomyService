package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	DatabaseURL   string `json:"database_url"`
	RedisAddr     string `json:"redis_addr"`
	RedisPassword string `json:"redis_password"`
	MetricsPort   string `json:"metrics_port"`
	APIPort       string `json:"api_port"`
}

func loadConfig() Config {
	config := Config{
		DatabaseURL:   "postgres://postgres:password@localhost:5432/minigames",
		RedisAddr:     "localhost:6379",
		RedisPassword: "",
		MetricsPort:   ":8083",
		APIPort:       ":8084",
	}

	file, err := os.Open("config.json")
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("config.json not found, creating default configuration...")
			defaultData, _ := json.MarshalIndent(config, "", "  ")
			if writeErr := os.WriteFile("config.json", defaultData, 0644); writeErr != nil {
				log.Printf("Failed to create default config.json: %v", writeErr)
			}
		} else {
			log.Printf("Error reading config.json: %v, using defaults", err)
		}
	} else {
		defer file.Close()
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&config); err != nil {
			log.Printf("Error decoding config.json, using defaults: %v", err)
		} else {
			log.Println("Loaded configuration from config.json")
		}
	}

	if envDB := os.Getenv("DATABASE_URL"); envDB != "" {
		config.DatabaseURL = envDB
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		config.RedisAddr = envRedis
	}
	if envRedisPass := os.Getenv("REDIS_PASSWORD"); envRedisPass != "" {
		config.RedisPassword = envRedisPass
	}
	if envMetrics := os.Getenv("METRICS_PORT"); envMetrics != "" {
		config.MetricsPort = envMetrics
	}
	if envAPI := os.Getenv("API_PORT"); envAPI != "" {
		config.APIPort = envAPI
	}

	return config
}

func main() {
	log.Println("===========================================")
	log.Println(" Starting Minigames EconomyService (Go)  ")
	log.Println("===========================================")

	config := loadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Database
	db, err := NewDatabase(ctx, config.DatabaseURL)
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

	// Initialize Redis Handler
	redisHandler := NewRedisHandler(config.RedisAddr, config.RedisPassword, db, workerID, &wg)
	defer redisHandler.Close()
	log.Println("Successfully connected to Redis")

	// Initialize REST API Server
	apiServer := NewAPIServer(db, redisHandler, config.APIPort)
	apiServer.Start(ctx)

	// Initialize Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Starting Prometheus metrics on %s", config.MetricsPort)
		if err := http.ListenAndServe(config.MetricsPort, nil); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Metrics server failed: %v", err)
		}
	}()

	// Start Redis Stream Listener
	wg.Add(1)
	go redisHandler.StartListening(ctx)

	log.Println("EconomyService is fully operational and waiting for match results & level up events...")

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutdown signal received, closing EconomyService gracefully...")
	cancel()
	redisHandler.Close()
	wg.Wait()
	log.Println("EconomyService shutdown complete.")
}
