package pubsub

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"economy-service/internal/domain"
	"economy-service/internal/metrics"
)

func (s *Subscriber) startClaimDailyListener(ctx context.Context) {
	defer s.wg.Done()
	log.Println("[PubSub] Listening for claim daily commands: economy:commands:claim_daily")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := s.redisClient.RawClient().Subscribe(ctx, "economy:commands:claim_daily")
		go func() {
			<-ctx.Done()
			_ = pubsub.Close()
		}()

		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				pubsub.Close()
				if ctx.Err() != nil {
					return
				}
				log.Printf("[PubSub ClaimDaily] Connection lost (%v), reconnecting in 3s...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				break
			}

			var req struct {
				UUID string `json:"uuid"`
			}
			if err := json.Unmarshal([]byte(msg.Payload), &req); err != nil || req.UUID == "" {
				continue
			}

			res, err := s.db.ClaimDailyBonus(ctx, req.UUID)
			if err != nil {
				log.Printf("[PubSub ClaimDaily] Failed claim for %s: %v", req.UUID, err)
				_ = s.publisher.PublishNotification(ctx, domain.EconomyNotification{
					UUID:      req.UUID,
					Source:    "DAILY_BONUS_FAILED",
					Timestamp: time.Now().Unix(),
				})
				continue
			}

			metrics.DailyClaimsTotal.WithLabelValues(strconv.Itoa(res.StreakDay)).Inc()
			metrics.CoinsAddedTotal.WithLabelValues("DAILY_BONUS").Add(float64(res.CoinsAwarded))

			updatedEco, _ := s.db.GetPlayerEconomy(ctx, req.UUID)
			if updatedEco != nil {
				_ = s.cacheRepo.CachePlayerEconomy(ctx, updatedEco)
				_ = s.leaderboard.UpdateLeaderboard(ctx, req.UUID, updatedEco.Coins)
			}

			_ = s.publisher.PublishNotification(ctx, domain.EconomyNotification{
				UUID:                 req.UUID,
				Coins:                res.NewTotalCoins,
				SeasonalTokens:       res.NewTotalTokens,
				ChangeCoins:          res.CoinsAwarded,
				ChangeSeasonalTokens: res.TokensAwarded,
				Source:               "DAILY_BONUS",
				Timestamp:            time.Now().Unix(),
			})
		}
	}
}
