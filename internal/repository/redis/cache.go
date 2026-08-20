package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"economy-service/internal/domain"
)

type CacheRepository struct {
	client *Client
}

func NewCacheRepository(client *Client) *CacheRepository {
	return &CacheRepository{client: client}
}

// CachePlayerEconomy caches profile into Redis Hash "player:economy:<uuid>"
func (r *CacheRepository) CachePlayerEconomy(ctx context.Context, p *domain.PlayerEconomy) error {
	key := fmt.Sprintf("player:economy:%s", p.PlayerUUID)
	var lastClaimUnix int64
	if p.LastDailyClaimAt != nil {
		lastClaimUnix = p.LastDailyClaimAt.Unix()
	}

	data := map[string]interface{}{
		"coins":               p.Coins,
		"seasonal_tokens":     p.SeasonalTokens,
		"login_streak":        p.LoginStreak,
		"can_claim_daily":     p.CanClaimDaily,
		"last_daily_claim_at": lastClaimUnix,
		"updated_at":          p.UpdatedAt.Unix(),
	}
	if err := r.client.RawClient().HSet(ctx, key, data).Err(); err != nil {
		return err
	}
	return r.client.RawClient().Expire(ctx, key, 24*time.Hour).Err()
}

// GetCachedPlayerEconomy retrieves profile from Redis cache
func (r *CacheRepository) GetCachedPlayerEconomy(ctx context.Context, playerUUID string) (*domain.PlayerEconomy, error) {
	key := fmt.Sprintf("player:economy:%s", playerUUID)
	res, err := r.client.RawClient().HGetAll(ctx, key).Result()
	if err != nil || len(res) == 0 {
		return nil, fmt.Errorf("cache miss")
	}

	coins, _ := strconv.ParseInt(res["coins"], 10, 64)
	tokens, _ := strconv.Atoi(res["seasonal_tokens"])
	streak, _ := strconv.Atoi(res["login_streak"])
	canClaim, _ := strconv.ParseBool(res["can_claim_daily"])

	var lastClaim *time.Time
	if claimTsStr, ok := res["last_daily_claim_at"]; ok && claimTsStr != "0" && claimTsStr != "" {
		if ts, err := strconv.ParseInt(claimTsStr, 10, 64); err == nil && ts > 0 {
			t := time.Unix(ts, 0)
			lastClaim = &t
		}
	}

	return &domain.PlayerEconomy{
		PlayerUUID:       playerUUID,
		Coins:            coins,
		SeasonalTokens:   tokens,
		LoginStreak:      streak,
		LastDailyClaimAt: lastClaim,
		CanClaimDaily:    canClaim,
	}, nil
}

// InvalidatePlayerEconomyCache purges economy profile cache from Redis
func (r *CacheRepository) InvalidatePlayerEconomyCache(ctx context.Context, playerUUID string) error {
	key := fmt.Sprintf("player:economy:%s", playerUUID)
	return r.client.RawClient().Del(ctx, key).Err()
}
