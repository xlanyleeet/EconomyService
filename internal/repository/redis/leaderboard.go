package redis

import (
	"context"
	"fmt"
	"log"

	"economy-service/internal/repository/postgres"

	goredis "github.com/redis/go-redis/v9"
)

type LeaderboardEntry struct {
	Rank       int    `json:"rank"`
	PlayerUUID string `json:"player_uuid"`
	Coins      int64  `json:"coins"`
}

type LeaderboardRepository struct {
	client *Client
}

func NewLeaderboardRepository(client *Client) *LeaderboardRepository {
	return &LeaderboardRepository{client: client}
}

// UpdateLeaderboard updates Redis Sorted Set "leaderboard:economy:coins"
func (r *LeaderboardRepository) UpdateLeaderboard(ctx context.Context, playerUUID string, coins int64) error {
	return r.client.RawClient().ZAdd(ctx, "leaderboard:economy:coins", goredis.Z{
		Score:  float64(coins),
		Member: playerUUID,
	}).Err()
}

// GetTopLeaderboard retrieves top N players from Redis Sorted Set
func (r *LeaderboardRepository) GetTopLeaderboard(ctx context.Context, limit int64) ([]LeaderboardEntry, error) {
	if limit <= 0 {
		limit = 10
	}
	top, err := r.client.RawClient().ZRevRangeWithScores(ctx, "leaderboard:economy:coins", 0, limit-1).Result()
	if err != nil {
		return nil, err
	}

	var entries []LeaderboardEntry
	for i, z := range top {
		entries = append(entries, LeaderboardEntry{
			Rank:       i + 1,
			PlayerUUID: fmt.Sprintf("%v", z.Member),
			Coins:      int64(z.Score),
		})
	}
	return entries, nil
}

// SyncLeaderboardFromDB populates Redis leaderboard from PostgreSQL top balances
func (r *LeaderboardRepository) SyncLeaderboardFromDB(ctx context.Context, db *postgres.Database) error {
	players, err := db.GetTopPlayers(ctx, 100)
	if err != nil {
		return err
	}
	if len(players) == 0 {
		return nil
	}
	var members []goredis.Z
	for _, p := range players {
		members = append(members, goredis.Z{
			Score:  float64(p.Coins),
			Member: p.PlayerUUID,
		})
	}
	err = r.client.RawClient().ZAdd(ctx, "leaderboard:economy:coins", members...).Err()
	if err == nil {
		log.Printf("Successfully synced %d top player balances to Redis leaderboard", len(players))
	}
	return err
}
