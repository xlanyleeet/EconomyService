package postgres

import (
	"context"
	"fmt"
	"log"
	"time"

	"economy-service/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// GetPlayerEconomy loads economy profile from DB
func (db *Database) GetPlayerEconomy(ctx context.Context, playerUUID string) (*domain.PlayerEconomy, error) {
	query := `
		SELECT player_uuid, coins, seasonal_tokens, login_streak, last_login_at, last_daily_claim_at, updated_at
		FROM player_economy
		WHERE player_uuid = $1::uuid
	`
	row := db.pool.QueryRow(ctx, query, playerUUID)

	var p domain.PlayerEconomy
	var lastClaim *time.Time

	err := row.Scan(&p.PlayerUUID, &p.Coins, &p.SeasonalTokens, &p.LoginStreak, &p.LastLoginAt, &lastClaim, &p.UpdatedAt)
	if err == pgx.ErrNoRows {
		p.PlayerUUID = playerUUID
		p.Coins = 0
		p.SeasonalTokens = 0
		p.LoginStreak = 0
		p.LastLoginAt = time.Now()
		p.CanClaimDaily = true
		p.UpdatedAt = time.Now()
		return &p, nil
	} else if err != nil {
		return nil, err
	}

	p.LastDailyClaimAt = lastClaim
	_, p.CanClaimDaily = domain.EvaluateDailyStreak(lastClaim, p.LoginStreak, time.Now())
	return &p, nil
}

// SetBalance sets absolute coins & seasonal_tokens in PostgreSQL DB
func (db *Database) SetBalance(ctx context.Context, playerUUID string, coins int64, seasonalTokens int) error {
	query := `
		INSERT INTO player_economy (player_uuid, coins, seasonal_tokens, updated_at)
		VALUES ($1::uuid, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (player_uuid) DO UPDATE SET
			coins = EXCLUDED.coins,
			seasonal_tokens = EXCLUDED.seasonal_tokens,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err := db.pool.Exec(ctx, query, playerUUID, coins, seasonalTokens)
	return err
}

// AddBalance executes an atomic transaction for mutating player balance
func (db *Database) AddBalance(ctx context.Context, playerUUID, currency string, amount int64, source, idempotencyKey string) (*domain.PlayerEconomy, error) {
	if amount == 0 {
		return db.GetPlayerEconomy(ctx, playerUUID)
	}

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Idempotency check
	if idempotencyKey != "" {
		var existingID string
		errCheck := tx.QueryRow(ctx, "SELECT transaction_id FROM economy_transactions WHERE idempotency_key = $1", idempotencyKey).Scan(&existingID)
		if errCheck == nil {
			log.Printf("Idempotent skip for key %s", idempotencyKey)
			_ = tx.Commit(ctx)
			return db.GetPlayerEconomy(ctx, playerUUID)
		}
	}

	// Ensure player row exists so SELECT FOR UPDATE acquires a row lock
	_, _ = tx.Exec(ctx, "INSERT INTO player_economy (player_uuid) VALUES ($1::uuid) ON CONFLICT (player_uuid) DO NOTHING", playerUUID)

	querySelect := `
		SELECT coins, seasonal_tokens, login_streak, last_daily_claim_at
		FROM player_economy
		WHERE player_uuid = $1::uuid
		FOR UPDATE
	`
	var currentCoins int64
	var currentTokens int
	var loginStreak int
	var lastClaim *time.Time

	err = tx.QueryRow(ctx, querySelect, playerUUID).Scan(&currentCoins, &currentTokens, &loginStreak, &lastClaim)
	if err != nil {
		return nil, err
	}

	var newCoins = currentCoins
	var newTokens = currentTokens
	var balanceAfter int64

	if currency == "coins" {
		newCoins += amount
		if newCoins < 0 {
			return nil, fmt.Errorf("insufficient funds (coins: %d, attempt subtract: %d)", currentCoins, amount)
		}
		balanceAfter = newCoins
	} else if currency == "seasonal_tokens" {
		newTokens += int(amount)
		if newTokens < 0 {
			return nil, fmt.Errorf("insufficient funds (seasonal_tokens: %d, attempt subtract: %d)", currentTokens, amount)
		}
		balanceAfter = int64(newTokens)
	} else {
		return nil, fmt.Errorf("unsupported currency: %s", currency)
	}

	queryUpsert := `
		INSERT INTO player_economy (player_uuid, coins, seasonal_tokens, updated_at)
		VALUES ($1::uuid, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (player_uuid) DO UPDATE SET
			coins = EXCLUDED.coins,
			seasonal_tokens = EXCLUDED.seasonal_tokens,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err = tx.Exec(ctx, queryUpsert, playerUUID, newCoins, newTokens)
	if err != nil {
		return nil, err
	}

	txID := uuid.New().String()
	queryAudit := `
		INSERT INTO economy_transactions (transaction_id, player_uuid, currency, amount, balance_after, source, idempotency_key)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7)
	`
	var idempParam *string
	if idempotencyKey != "" {
		idempParam = &idempotencyKey
	}
	_, err = tx.Exec(ctx, queryAudit, txID, playerUUID, currency, amount, balanceAfter, source, idempParam)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	_, canClaim := domain.EvaluateDailyStreak(lastClaim, loginStreak, time.Now())

	return &domain.PlayerEconomy{
		PlayerUUID:       playerUUID,
		Coins:            newCoins,
		SeasonalTokens:   newTokens,
		LoginStreak:      loginStreak,
		LastDailyClaimAt: lastClaim,
		CanClaimDaily:    canClaim,
		UpdatedAt:        time.Now(),
	}, nil
}
