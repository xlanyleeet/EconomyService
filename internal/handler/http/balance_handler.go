package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"economy-service/internal/domain"
	"economy-service/internal/metrics"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type TransactionRequest struct {
	UUID           string `json:"uuid"`
	Currency       string `json:"currency"` // "coins" or "seasonal_tokens"
	Amount         int64  `json:"amount"`   // Positive or negative
	Source         string `json:"source"`   // e.g. "SHOP_BUY", "QUEST_REWARD"
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

func (s *APIServer) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	playerUUID := chi.URLParam(r, "uuid")
	if playerUUID == "" {
		playerUUID = r.URL.Query().Get("uuid")
	}
	if playerUUID == "" {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: "Missing required 'uuid'"})
		return
	}

	if _, err := uuid.Parse(playerUUID); err != nil {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: "Invalid 'uuid' format"})
		return
	}

	ctx := r.Context()
	if s.cacheRepo != nil {
		cached, err := s.cacheRepo.GetCachedPlayerEconomy(ctx, playerUUID)
		if err == nil && cached != nil {
			writeJSON(w, http.StatusOK, domain.APIResponse{Success: true, Data: cached})
			return
		}
	}

	profile, err := s.db.GetPlayerEconomy(ctx, playerUUID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, domain.APIResponse{Success: false, Message: fmt.Sprintf("Failed to load balance: %v", err)})
		return
	}

	if s.cacheRepo != nil {
		_ = s.cacheRepo.CachePlayerEconomy(ctx, profile)
	}
	writeJSON(w, http.StatusOK, domain.APIResponse{Success: true, Data: profile})
}

func (s *APIServer) handleTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, domain.APIResponse{Success: false, Message: "Method not allowed"})
		return
	}

	var req TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UUID == "" || req.Currency == "" {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: "Invalid payload. 'uuid' and 'currency' are required"})
		return
	}

	if _, err := uuid.Parse(req.UUID); err != nil {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: "Invalid 'uuid' format"})
		return
	}

	if req.Amount == 0 {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: "Amount cannot be 0"})
		return
	}

	if req.Amount > 1000000000 || req.Amount < -1000000000 {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: "Amount exceeds allowable limits (-1B to +1B)"})
		return
	}

	ctx := r.Context()
	updatedEco, err := s.db.AddBalance(ctx, req.UUID, req.Currency, req.Amount, req.Source, req.IdempotencyKey)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: err.Error()})
		return
	}

	metrics.TransactionsProcessedTotal.WithLabelValues(req.Currency, req.Source).Inc()
	if req.Currency == "coins" && req.Amount > 0 {
		metrics.CoinsAddedTotal.WithLabelValues(req.Source).Add(float64(req.Amount))
	}

	if s.cacheRepo != nil {
		_ = s.cacheRepo.CachePlayerEconomy(ctx, updatedEco)
	}
	if s.leaderboard != nil {
		_ = s.leaderboard.UpdateLeaderboard(ctx, req.UUID, updatedEco.Coins)
	}

	changeCoins := int64(0)
	changeTokens := 0
	if req.Currency == "coins" {
		changeCoins = req.Amount
	} else if req.Currency == "seasonal_tokens" {
		changeTokens = int(req.Amount)
	}

	if s.publisher != nil {
		_ = s.publisher.PublishNotification(ctx, domain.EconomyNotification{
			UUID:                 req.UUID,
			Coins:                updatedEco.Coins,
			SeasonalTokens:       updatedEco.SeasonalTokens,
			ChangeCoins:          changeCoins,
			ChangeSeasonalTokens: changeTokens,
			Source:               req.Source,
			Timestamp:            time.Now().Unix(),
		})
	}

	writeJSON(w, http.StatusOK, domain.APIResponse{Success: true, Data: updatedEco})
}
