package http

import (
	"net/http"

	"economy-service/internal/domain"
)

func (s *APIServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey != "" {
			token := r.Header.Get("X-API-Key")
			if token == "" {
				token = r.Header.Get("Authorization")
			}
			if token != s.apiKey && token != "Bearer "+s.apiKey {
				writeJSON(w, http.StatusUnauthorized, domain.APIResponse{Success: false, Message: "Unauthorized: Invalid API key"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
