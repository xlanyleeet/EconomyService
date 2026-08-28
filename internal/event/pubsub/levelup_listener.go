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

func (s *Subscriber) startLevelUpListener(ctx context.Context) {
	defer s.wg.Done()
	log.Println("[PubSub] Listening for LevelUp rewards: leveling:events:levelup")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := s.redisClient.RawClient().Subscribe(ctx, "leveling:events:levelup")
		ch := pubsub.Channel()

		done := false
		for !done {
			select {
			case <-ctx.Done():
				_ = pubsub.Close()
				return
			case msg, ok := <-ch:
				if !ok {
					_ = pubsub.Close()
					if ctx.Err() != nil {
						return
					}
					log.Printf("[PubSub LevelUp] Connection lost, reconnecting in 3s...")
					done = true
					break
				}

				if msg == nil || msg.Payload == "" {
					continue
				}

				var event domain.LevelUpEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					log.Printf("[PubSub LevelUp] Parse error: %v", err)
					continue
				}

				s.processLevelUpRewards(ctx, event)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (s *Subscriber) processLevelUpRewards(ctx context.Context, event domain.LevelUpEvent) {
	totalCoinsReward := int64(0)
	for _, reward := range event.UnlockedRewards {
		totalCoinsReward += reward.Coins
	}

	if totalCoinsReward <= 0 {
		totalCoinsReward = int64(event.NewLevel * 100)
	}

	idempKey := "levelup-" + event.PlayerUUID + "-lvl" + strconv.Itoa(event.NewLevel)
	updatedEco, err := s.db.AddBalance(ctx, event.PlayerUUID, "coins", totalCoinsReward, "LEVEL_UP", idempKey)
	if err != nil {
		log.Printf("[PubSub LevelUp] Error adding balance for %s: %v", event.PlayerUUID, err)
		return
	}

	log.Printf("💰 Awarded %d Level Up coins to %s (Level %d)", totalCoinsReward, event.PlayerUUID, event.NewLevel)

	metrics.CoinsAddedTotal.WithLabelValues("LEVEL_UP").Add(float64(totalCoinsReward))
	metrics.TransactionsProcessedTotal.WithLabelValues("coins", "LEVEL_UP").Inc()

	_ = s.cacheRepo.CachePlayerEconomy(ctx, updatedEco)
	_ = s.leaderboard.UpdateLeaderboard(ctx, event.PlayerUUID, updatedEco.Coins)
	_ = s.publisher.PublishNotification(ctx, domain.EconomyNotification{
		UUID:                 event.PlayerUUID,
		Coins:                updatedEco.Coins,
		SeasonalTokens:       updatedEco.SeasonalTokens,
		ChangeCoins:          totalCoinsReward,
		ChangeSeasonalTokens: 0,
		Source:               "LEVEL_UP",
		Timestamp:            time.Now().Unix(),
	})
}
