package main

import (
	"time"
)

// PlayerEconomy represents the persistent economy state in PostgreSQL & Redis
type PlayerEconomy struct {
	PlayerUUID       string     `json:"player_uuid"`
	Coins            int64      `json:"coins"`
	SeasonalTokens   int        `json:"seasonal_tokens"`
	LoginStreak      int        `json:"login_streak"`
	LastLoginAt      time.Time  `json:"last_login_at"`
	LastDailyClaimAt *time.Time `json:"last_daily_claim_at,omitempty"`
	CanClaimDaily    bool       `json:"can_claim_daily"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// EconomyTransaction represents an audit record of balance mutation
type EconomyTransaction struct {
	TransactionID  string    `json:"transaction_id"`
	PlayerUUID     string    `json:"player_uuid"`
	Currency       string    `json:"currency"` // "coins" or "seasonal_tokens"
	Amount         int64     `json:"amount"`   // Positive (add) or negative (subtract)
	BalanceAfter   int64     `json:"balance_after"`
	Source         string    `json:"source"`   // "MATCH_WIN", "DAILY_BONUS", "LEVEL_UP", "SHOP_BUY"
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// DailyBonusReward describes the reward for a specific streak day
type DailyBonusReward struct {
	Day            int    `json:"day"`
	Coins          int64  `json:"coins"`
	SeasonalTokens int    `json:"seasonal_tokens"`
	ChestReward    string `json:"chest_reward,omitempty"`
}

// DailyBonusClaimResult describes the result of claiming a daily bonus
type DailyBonusClaimResult struct {
	PlayerUUID      string           `json:"player_uuid"`
	StreakDay       int              `json:"streak_day"`
	CoinsAwarded    int64            `json:"coins_awarded"`
	TokensAwarded   int              `json:"tokens_awarded"`
	NewTotalCoins   int64            `json:"new_total_coins"`
	NewTotalTokens  int              `json:"new_total_tokens"`
	NextClaimAvailableAt time.Time   `json:"next_claim_available_at"`
}

// MatchResult represents the incoming JSON event from Redis Stream "minigames:events:match_results"
type MatchResult struct {
	MatchID         string            `json:"match_id"`
	ArenaName       string            `json:"arena_name"`
	GameMode        string            `json:"game_mode"`
	DurationSeconds int               `json:"duration_seconds"`
	Players         []PlayerMatchData `json:"players"`
}

// PlayerMatchData contains match results per player
type PlayerMatchData struct {
	UUID          string            `json:"uuid"`
	Status        string            `json:"status"` // "WINNER" or "LOSER"
	Rank          string            `json:"rank,omitempty"`
	EarnedEconomy EarnedEconomyData `json:"earned_economy"`
}

// EarnedEconomyData contains raw XP and coins earned
type EarnedEconomyData struct {
	Coins int64 `json:"coins"`
	XP    int64 `json:"xp"`
}

// LevelUpEvent represents event from Redis Stream/PubSub "leveling:events:levelup"
type LevelUpEvent struct {
	PlayerUUID      string       `json:"player_uuid"`
	OldLevel        int          `json:"old_level"`
	NewLevel        int          `json:"new_level"`
	Prestige        int          `json:"prestige"`
	UnlockedRewards []LevelReward `json:"unlocked_rewards"`
	Timestamp       int64        `json:"timestamp"`
}

// LevelReward defines unlocked rewards from LevelingService
type LevelReward struct {
	Level      int    `json:"level"`
	Coins      int64  `json:"coins"`
	CosmeticID string `json:"cosmetic_id,omitempty"`
	Prefix     string `json:"prefix,omitempty"`
	Title      string `json:"title,omitempty"`
}

// EconomyNotification is published to Redis Pub/Sub "economy:notifications"
type EconomyNotification struct {
	UUID                 string `json:"uuid"`
	Coins                int64  `json:"coins"`
	SeasonalTokens       int    `json:"seasonal_tokens"`
	ChangeCoins          int64  `json:"change_coins"`
	ChangeSeasonalTokens int    `json:"change_seasonal_tokens"`
	Source               string `json:"source"`
	Timestamp            int64  `json:"timestamp"`
}

// APIResponse standardizes HTTP REST API responses
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}
