package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

type APIServer struct {
	db           *Database
	redisHandler *RedisHandler
	port         string
}

func NewAPIServer(db *Database, redisHandler *RedisHandler, port string) *APIServer {
	return &APIServer{
		db:           db,
		redisHandler: redisHandler,
		port:         port,
	}
}

func (s *APIServer) Start(ctx context.Context) {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/v1/economy/balance", s.handleGetBalance)
	mux.HandleFunc("/api/v1/economy/transaction", s.handleTransaction)
	mux.HandleFunc("/api/v1/economy/daily-bonus/claim", s.handleClaimDailyBonus)
	mux.HandleFunc("/api/v1/economy/leaderboard", s.handleLeaderboard)

	server := &http.Server{
		Addr:         s.port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting Economy REST API server on %s", s.port)
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

func writeJSON(w http.ResponseWriter, status int, resp APIResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Message: "EconomyService is healthy"})
}

func (s *APIServer) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	uuid := r.URL.Query().Get("uuid")
	if uuid == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Missing required query param 'uuid'"})
		return
	}

	ctx := r.Context()
	cached, err := s.redisHandler.GetCachedPlayerEconomy(ctx, uuid)
	if err == nil && cached != nil {
		writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: cached})
		return
	}

	profile, err := s.db.GetPlayerEconomy(ctx, uuid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: fmt.Sprintf("Failed to load balance: %v", err)})
		return
	}

	s.redisHandler.CachePlayerEconomy(ctx, profile)
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: profile})
}

type TransactionRequest struct {
	UUID           string `json:"uuid"`
	Currency       string `json:"currency"` // "coins" or "seasonal_tokens"
	Amount         int64  `json:"amount"`   // Positive or negative
	Source         string `json:"source"`   // e.g. "SHOP_BUY", "QUEST_REWARD"
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

func (s *APIServer) handleTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}

	var req TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UUID == "" || req.Currency == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Invalid payload. 'uuid' and 'currency' are required"})
		return
	}

	ctx := r.Context()
	updatedEco, err := s.db.AddBalance(ctx, req.UUID, req.Currency, req.Amount, req.Source, req.IdempotencyKey)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: err.Error()})
		return
	}

	transactionsProcessedTotal.WithLabelValues(req.Currency, req.Source).Inc()
	if req.Currency == "coins" && req.Amount > 0 {
		coinsAddedTotal.WithLabelValues(req.Source).Add(float64(req.Amount))
	}

	s.redisHandler.CachePlayerEconomy(ctx, updatedEco)
	s.redisHandler.UpdateLeaderboard(ctx, req.UUID, updatedEco.Coins)

	changeCoins := int64(0)
	changeTokens := 0
	if req.Currency == "coins" {
		changeCoins = req.Amount
	} else if req.Currency == "seasonal_tokens" {
		changeTokens = int(req.Amount)
	}

	s.redisHandler.PublishNotification(ctx, EconomyNotification{
		UUID:                 req.UUID,
		Coins:                updatedEco.Coins,
		SeasonalTokens:       updatedEco.SeasonalTokens,
		ChangeCoins:          changeCoins,
		ChangeSeasonalTokens: changeTokens,
		Source:               req.Source,
		Timestamp:            time.Now().Unix(),
	})

	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: updatedEco})
}

func (s *APIServer) handleClaimDailyBonus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, APIResponse{Success: false, Message: "Method not allowed"})
		return
	}

	uuid := r.URL.Query().Get("uuid")
	if uuid == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: "Missing required query param 'uuid'"})
		return
	}

	ctx := r.Context()
	res, err := s.db.ClaimDailyBonus(ctx, uuid)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Success: false, Message: err.Error()})
		return
	}

	dailyClaimsTotal.WithLabelValues(strconv.Itoa(res.StreakDay)).Inc()
	coinsAddedTotal.WithLabelValues("DAILY_BONUS").Add(float64(res.CoinsAwarded))

	updatedEco, _ := s.db.GetPlayerEconomy(ctx, uuid)
	if updatedEco != nil {
		s.redisHandler.CachePlayerEconomy(ctx, updatedEco)
		s.redisHandler.UpdateLeaderboard(ctx, uuid, updatedEco.Coins)
	}

	s.redisHandler.PublishNotification(ctx, EconomyNotification{
		UUID:                 uuid,
		Coins:                res.NewTotalCoins,
		SeasonalTokens:       res.NewTotalTokens,
		ChangeCoins:          res.CoinsAwarded,
		ChangeSeasonalTokens: res.TokensAwarded,
		Source:               "DAILY_BONUS",
		Timestamp:            time.Now().Unix(),
	})

	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: res})
}

func (s *APIServer) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	top, err := s.redisHandler.client.ZRevRangeWithScores(ctx, "leaderboard:economy:coins", 0, 9).Result()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Success: false, Message: "Failed to fetch leaderboard"})
		return
	}

	type LeaderboardEntry struct {
		Rank       int    `json:"rank"`
		PlayerUUID string `json:"player_uuid"`
		Coins      int64  `json:"coins"`
	}

	var entries []LeaderboardEntry
	for i, z := range top {
		entries = append(entries, LeaderboardEntry{
			Rank:       i + 1,
			PlayerUUID: z.Member.(string),
			Coins:      int64(z.Score),
		})
	}

	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: entries})
}
