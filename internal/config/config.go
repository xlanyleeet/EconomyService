package config

import (
	"encoding/json"
	"log"
	"os"
)

type Config struct {
	DatabaseURL   string `json:"database_url"`
	RedisAddr     string `json:"redis_addr"`
	RedisPassword string `json:"redis_password"`
	MetricsPort   string `json:"metrics_port"`
	APIPort       string `json:"api_port"`
	APIKey        string `json:"api_key,omitempty"`
}

func LoadConfig() Config {
	cfg := Config{
		DatabaseURL:   "postgres://postgres:password@localhost:5432/minigames",
		RedisAddr:     "localhost:6379",
		RedisPassword: "",
		MetricsPort:   ":8083",
		APIPort:       ":8084",
		APIKey:        "",
	}

	file, err := os.Open("config.json")
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("config.json not found, creating default configuration...")
			defaultData, _ := json.MarshalIndent(cfg, "", "  ")
			if writeErr := os.WriteFile("config.json", defaultData, 0644); writeErr != nil {
				log.Printf("Failed to create default config.json: %v", writeErr)
			}
		} else {
			log.Printf("Error reading config.json: %v, using defaults", err)
		}
	} else {
		defer file.Close()
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&cfg); err != nil {
			log.Printf("Error decoding config.json, using defaults: %v", err)
		} else {
			log.Println("Loaded configuration from config.json")
		}
	}

	if envDB := os.Getenv("DATABASE_URL"); envDB != "" {
		cfg.DatabaseURL = envDB
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		cfg.RedisAddr = envRedis
	}
	if envRedisPass := os.Getenv("REDIS_PASSWORD"); envRedisPass != "" {
		cfg.RedisPassword = envRedisPass
	}
	if envMetrics := os.Getenv("METRICS_PORT"); envMetrics != "" {
		cfg.MetricsPort = envMetrics
	}
	if envAPI := os.Getenv("API_PORT"); envAPI != "" {
		cfg.APIPort = envAPI
	}
	if envAPIKey := os.Getenv("API_KEY"); envAPIKey != "" {
		cfg.APIKey = envAPIKey
	}

	return cfg
}
