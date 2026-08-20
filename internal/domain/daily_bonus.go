package domain

import (
	"time"
)

// GetDailyReward returns reward for a given streak day (1 to 7)
func GetDailyReward(day int) DailyBonusReward {
	if day < 1 {
		day = 1
	}
	if day > 7 {
		day = 7
	}

	rewards := map[int]DailyBonusReward{
		1: {Day: 1, Coins: 500, SeasonalTokens: 0},
		2: {Day: 2, Coins: 750, SeasonalTokens: 0},
		3: {Day: 3, Coins: 1000, SeasonalTokens: 10},
		4: {Day: 4, Coins: 1500, SeasonalTokens: 15},
		5: {Day: 5, Coins: 2000, SeasonalTokens: 25},
		6: {Day: 6, Coins: 3000, SeasonalTokens: 50},
		7: {Day: 7, Coins: 5000, SeasonalTokens: 100, ChestReward: "COSMIC_CHEST"},
	}

	return rewards[day]
}

// EvaluateDailyStreak determines next streak day and whether bonus can be claimed
func EvaluateDailyStreak(lastClaim *time.Time, currentStreak int, now time.Time) (nextStreakDay int, canClaim bool) {
	if lastClaim == nil || lastClaim.IsZero() {
		return 1, true
	}

	timeSinceLastClaim := now.Sub(*lastClaim)

	// Less than 20 hours since last claim (Cooldown)
	if timeSinceLastClaim < 20*time.Hour {
		return currentStreak, false
	}

	// Between 20h and 48h (Valid consecutive day)
	if timeSinceLastClaim <= 48*time.Hour {
		if currentStreak >= 7 {
			return 1, true // Cycle restarts after day 7
		}
		return currentStreak + 1, true
	}

	// Greater than 48 hours (Streak expired)
	return 1, true
}
