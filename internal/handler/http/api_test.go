package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"economy-service/internal/domain"

	"github.com/go-chi/chi/v5"
)

func TestAPIHealth(t *testing.T) {
	apiServer := NewAPIServer(nil, nil, nil, nil, ":8084", "")
	r := chi.NewRouter()
	r.Get("/health", apiServer.handleHealth)

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp domain.APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected success true, got false")
	}
}

func TestAPIGetBalanceValidation(t *testing.T) {
	apiServer := NewAPIServer(nil, nil, nil, nil, ":8084", "")
	r := chi.NewRouter()
	r.Get("/api/v1/economy/balance", apiServer.handleGetBalance)
	r.Get("/api/v1/economy/balance/{uuid}", apiServer.handleGetBalance)

	t.Run("Missing UUID", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/economy/balance", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rec.Code)
		}
	})

	t.Run("Invalid UUID format in query", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/economy/balance?uuid=not-a-valid-uuid", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rec.Code)
		}
	})

	t.Run("Invalid UUID format in path", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/economy/balance/not-a-valid-uuid", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rec.Code)
		}
	})
}

func TestAPIAuthMiddleware(t *testing.T) {
	apiKey := "test-secret-key"
	apiServer := NewAPIServer(nil, nil, nil, nil, ":8084", apiKey)

	r := chi.NewRouter()
	r.With(apiServer.authMiddleware).Post("/api/v1/economy/transaction", apiServer.handleTransaction)

	t.Run("Unauthorized request without key", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/economy/transaction", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", rec.Code)
		}
	})

	t.Run("Authorized request with valid key but zero amount", func(t *testing.T) {
		payload := map[string]interface{}{
			"uuid":     "550e8400-e29b-41d4-a716-446655440000",
			"currency": "coins",
			"amount":   0,
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest("POST", "/api/v1/economy/transaction", bytes.NewReader(body))
		req.Header.Set("X-API-Key", apiKey)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400 for zero amount, got %d", rec.Code)
		}
	})
}
