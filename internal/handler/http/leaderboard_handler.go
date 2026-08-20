package http

import (
	"net/http"

	"economy-service/internal/domain"
)

func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, domain.APIResponse{Success: true, Message: "EconomyService is healthy"})
}

func (s *APIServer) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if s.leaderboard == nil {
		writeJSON(w, http.StatusInternalServerError, domain.APIResponse{Success: false, Message: "Leaderboard uninitialized"})
		return
	}

	entries, err := s.leaderboard.GetTopLeaderboard(ctx, 10)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, domain.APIResponse{Success: false, Message: "Failed to fetch leaderboard"})
		return
	}

	writeJSON(w, http.StatusOK, domain.APIResponse{Success: true, Data: entries})
}
