package postgres

import (
	"context"
	"fmt"
	"time"

	"economy-service/internal/domain"

	"github.com/google/uuid"
)

// ClaimDailyBonus claims the daily streak bonus atomically
func (db *Database) ClaimDailyBonus(ctx context.Context, playerUUID string) (*domain.DailyBonusClaimResult, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

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
	var currentStreak int
	var lastClaim *time.Time

	err = tx.QueryRow(ctx, querySelect, playerUUID).Scan(&currentCoins, &currentTokens, &currentStreak, &lastClaim)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	nextStreakDay, canClaim := domain.EvaluateDailyStreak(lastClaim, currentStreak, now)
	if !canClaim {
		return nil, fmt.Errorf("daily bonus is not available yet (cooldown active)")
	}

	reward := domain.GetDailyReward(nextStreakDay)
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

	return &domain.DailyBonusClaimResult{
		PlayerUUID:           playerUUID,
		StreakDay:            nextStreakDay,
		CoinsAwarded:         reward.Coins,
		TokensAwarded:        reward.SeasonalTokens,
		NewTotalCoins:        newCoins,
		NewTotalTokens:       newTokens,
		NextClaimAvailableAt: now.Add(20 * time.Hour),
	}, nil
}
