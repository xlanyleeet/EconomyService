package http

import (
	"net/http"
	"strconv"
	"time"

	"economy-service/internal/domain"
	"economy-service/internal/metrics"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func (s *APIServer) handleClaimDailyBonus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, domain.APIResponse{Success: false, Message: "Method not allowed"})
		return
	}

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
	res, err := s.db.ClaimDailyBonus(ctx, playerUUID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, domain.APIResponse{Success: false, Message: err.Error()})
		return
	}

	metrics.DailyClaimsTotal.WithLabelValues(strconv.Itoa(res.StreakDay)).Inc()
	metrics.CoinsAddedTotal.WithLabelValues("DAILY_BONUS").Add(float64(res.CoinsAwarded))

	updatedEco, _ := s.db.GetPlayerEconomy(ctx, playerUUID)
	if updatedEco != nil {
		if s.cacheRepo != nil {
			_ = s.cacheRepo.CachePlayerEconomy(ctx, updatedEco)
		}
		if s.leaderboard != nil {
			_ = s.leaderboard.UpdateLeaderboard(ctx, playerUUID, updatedEco.Coins)
		}
	}

	if s.publisher != nil {
		_ = s.publisher.PublishNotification(ctx, domain.EconomyNotification{
			UUID:                 playerUUID,
			Coins:                res.NewTotalCoins,
			SeasonalTokens:       res.NewTotalTokens,
			ChangeCoins:          res.CoinsAwarded,
			ChangeSeasonalTokens: res.TokensAwarded,
			Source:               "DAILY_BONUS",
			Timestamp:            time.Now().Unix(),
		})
	}

	writeJSON(w, http.StatusOK, domain.APIResponse{Success: true, Data: res})
}
