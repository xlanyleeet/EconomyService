package domain

import (
	"testing"
	"time"
)

func TestGetDailyReward(t *testing.T) {
	tests := []struct {
		day            int
		expectedCoins  int64
		expectedTokens int
	}{
		{day: 0, expectedCoins: 500, expectedTokens: 0},
		{day: 1, expectedCoins: 500, expectedTokens: 0},
		{day: 3, expectedCoins: 1000, expectedTokens: 10},
		{day: 7, expectedCoins: 5000, expectedTokens: 100},
		{day: 10, expectedCoins: 5000, expectedTokens: 100},
	}

	for _, tt := range tests {
		reward := GetDailyReward(tt.day)
		if reward.Coins != tt.expectedCoins {
			t.Errorf("day %d: expected coins %d, got %d", tt.day, tt.expectedCoins, reward.Coins)
		}
		if reward.SeasonalTokens != tt.expectedTokens {
			t.Errorf("day %d: expected tokens %d, got %d", tt.day, tt.expectedTokens, reward.SeasonalTokens)
		}
	}
}

func TestEvaluateDailyStreak(t *testing.T) {
	now := time.Now()

	t.Run("Never claimed before", func(t *testing.T) {
		day, canClaim := EvaluateDailyStreak(nil, 0, now)
		if day != 1 || !canClaim {
			t.Errorf("expected (1, true), got (%d, %v)", day, canClaim)
		}
	})

	t.Run("Claimed 5 hours ago (Cooldown)", func(t *testing.T) {
		last := now.Add(-5 * time.Hour)
		day, canClaim := EvaluateDailyStreak(&last, 2, now)
		if day != 2 || canClaim {
			t.Errorf("expected (2, false), got (%d, %v)", day, canClaim)
		}
	})

	t.Run("Claimed 24 hours ago (Valid streak continuation)", func(t *testing.T) {
		last := now.Add(-24 * time.Hour)
		day, canClaim := EvaluateDailyStreak(&last, 2, now)
		if day != 3 || !canClaim {
			t.Errorf("expected (3, true), got (%d, %v)", day, canClaim)
		}
	})

	t.Run("Claimed 50 hours ago (Streak expired)", func(t *testing.T) {
		last := now.Add(-50 * time.Hour)
		day, canClaim := EvaluateDailyStreak(&last, 5, now)
		if day != 1 || !canClaim {
			t.Errorf("expected (1, true), got (%d, %v)", day, canClaim)
		}
	})
}
