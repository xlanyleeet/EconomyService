package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Database struct {
	pool *pgxpool.Pool
}

func NewDatabase(ctx context.Context, connString string) (*Database, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database config: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	db := &Database{pool: pool}
	if err := db.initTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to init database tables: %w", err)
	}

	return db, nil
}

func (db *Database) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

func (db *Database) initTables(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS player_economy (
		player_uuid UUID PRIMARY KEY,
		coins BIGINT NOT NULL DEFAULT 0,
		seasonal_tokens INT NOT NULL DEFAULT 0,
		login_streak INT NOT NULL DEFAULT 0,
		last_login_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		last_daily_claim_at TIMESTAMP WITH TIME ZONE,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS economy_transactions (
		transaction_id UUID PRIMARY KEY,
		player_uuid UUID NOT NULL,
		currency VARCHAR(32) NOT NULL,
		amount BIGINT NOT NULL,
		balance_after BIGINT NOT NULL,
		source VARCHAR(64) NOT NULL,
		idempotency_key VARCHAR(128) UNIQUE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_economy_transactions_player ON economy_transactions(player_uuid, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_player_economy_coins ON player_economy(coins DESC);
	`

	_, err := db.pool.Exec(ctx, schema)
	return err
}

// GetPlayerEconomy loads economy profile from DB
func (db *Database) GetPlayerEconomy(ctx context.Context, playerUUID string) (*PlayerEconomy, error) {
	query := `
		SELECT player_uuid, coins, seasonal_tokens, login_streak, last_login_at, last_daily_claim_at, updated_at
		FROM player_economy
		WHERE player_uuid = $1::uuid
	`
	row := db.pool.QueryRow(ctx, query, playerUUID)

	var p PlayerEconomy
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
	_, p.CanClaimDaily = EvaluateDailyStreak(lastClaim, p.LoginStreak, time.Now())
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
func (db *Database) AddBalance(ctx context.Context, playerUUID, currency string, amount int64, source, idempotencyKey string) (*PlayerEconomy, error) {
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

	// Select FOR UPDATE
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
	if err == pgx.ErrNoRows {
		currentCoins = 0
		currentTokens = 0
		loginStreak = 0
	} else if err != nil {
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

	_, canClaim := EvaluateDailyStreak(lastClaim, loginStreak, time.Now())

	return &PlayerEconomy{
		PlayerUUID:       playerUUID,
		Coins:            newCoins,
		SeasonalTokens:   newTokens,
		LoginStreak:      loginStreak,
		LastDailyClaimAt: lastClaim,
		CanClaimDaily:    canClaim,
		UpdatedAt:        time.Now(),
	}, nil
}

// ClaimDailyBonus claims the daily streak bonus atomically
func (db *Database) ClaimDailyBonus(ctx context.Context, playerUUID string) (*DailyBonusClaimResult, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	querySelect := `
		SELECT coins, seasonal_tokens, login_streak, last_daily_claim_at
		FROM player_economy
		WHERE player_uuid = $1::uuid
		FOR UPDATE
	`
	var currentCoins int64
	var currentTokens int
	var currentStreak int
	var lastClaim *time.Time

	err = tx.QueryRow(ctx, querySelect, playerUUID).Scan(&currentCoins, &currentTokens, &currentStreak, &lastClaim)
	if err != nil && err != pgx.ErrNoRows {
		return nil, err
	}

	now := time.Now()
	nextStreakDay, canClaim := EvaluateDailyStreak(lastClaim, currentStreak, now)
	if !canClaim {
		return nil, fmt.Errorf("daily bonus is not available yet (cooldown active)")
	}

	reward := GetDailyReward(nextStreakDay)
	newCoins := currentCoins + reward.Coins
	newTokens := currentTokens + reward.SeasonalTokens

	queryUpsert := `
		INSERT INTO player_economy (player_uuid, coins, seasonal_tokens, login_streak, last_daily_claim_at, updated_at)
		VALUES ($1::uuid, $2, $3, $4, $5, CURRENT_TIMESTAMP)
		ON CONFLICT (player_uuid) DO UPDATE SET
			coins = EXCLUDED.coins,
			seasonal_tokens = EXCLUDED.seasonal_tokens,
			login_streak = EXCLUDED.login_streak,
			last_daily_claim_at = EXCLUDED.last_daily_claim_at,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err = tx.Exec(ctx, queryUpsert, playerUUID, newCoins, newTokens, nextStreakDay, now)
	if err != nil {
		return nil, err
	}

	txID := uuid.New().String()
	idempKey := fmt.Sprintf("daily-claim-%s-%s", playerUUID, now.Format("2006-01-02"))
	_, err = tx.Exec(ctx, `
		INSERT INTO economy_transactions (transaction_id, player_uuid, currency, amount, balance_after, source, idempotency_key)
		VALUES ($1::uuid, $2::uuid, 'coins', $3, $4, 'DAILY_BONUS', $5)
	`, txID, playerUUID, reward.Coins, newCoins, idempKey)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &DailyBonusClaimResult{
		PlayerUUID:           playerUUID,
		StreakDay:            nextStreakDay,
		CoinsAwarded:         reward.Coins,
		TokensAwarded:        reward.SeasonalTokens,
		NewTotalCoins:        newCoins,
		NewTotalTokens:       newTokens,
		NextClaimAvailableAt: now.Add(20 * time.Hour),
	}, nil
}
