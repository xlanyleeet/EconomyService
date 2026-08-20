package main

import (
	"testing"
	"time"
)

func TestGetDailyReward(t *testing.T) {
	r1 := GetDailyReward(1)
	if r1.Coins != 500 || r1.SeasonalTokens != 0 {
		t.Errorf("Day 1 reward incorrect: %+v", r1)
	}

	r7 := GetDailyReward(7)
	if r7.Coins != 5000 || r7.SeasonalTokens != 100 || r7.ChestReward != "COSMIC_CHEST" {
		t.Errorf("Day 7 reward incorrect: %+v", r7)
	}
}

func TestEvaluateDailyStreak(t *testing.T) {
	now := time.Now()

	// Case 1: First time claiming
	nextDay, canClaim := EvaluateDailyStreak(nil, 0, now)
	if nextDay != 1 || !canClaim {
		t.Errorf("First claim failed: day=%d, canClaim=%v", nextDay, canClaim)
	}

	// Case 2: Claimed 5 hours ago (Cooldown)
	t5h := now.Add(-5 * time.Hour)
	nextDay, canClaim = EvaluateDailyStreak(&t5h, 1, now)
	if canClaim {
		t.Errorf("Should not be able to claim after 5 hours")
	}

	// Case 3: Claimed 25 hours ago (Consecutive day 2)
	t25h := now.Add(-25 * time.Hour)
	nextDay, canClaim = EvaluateDailyStreak(&t25h, 1, now)
	if nextDay != 2 || !canClaim {
		t.Errorf("Day 2 claim failed: day=%d, canClaim=%v", nextDay, canClaim)
	}

	// Case 4: Claimed 60 hours ago (Streak expired -> Reset to Day 1)
	t60h := now.Add(-60 * time.Hour)
	nextDay, canClaim = EvaluateDailyStreak(&t60h, 5, now)
	if nextDay != 1 || !canClaim {
		t.Errorf("Streak reset failed: day=%d, canClaim=%v", nextDay, canClaim)
	}
}
