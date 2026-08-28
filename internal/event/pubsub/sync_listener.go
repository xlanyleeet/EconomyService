package pubsub

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

func (s *Subscriber) startSyncBalanceListener(ctx context.Context) {
	defer s.wg.Done()
	log.Println("[PubSub] Listening for sync balance commands: economy:commands:sync")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := s.redisClient.RawClient().Subscribe(ctx, "economy:commands:sync")
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
					log.Printf("[PubSub Sync] Connection lost, reconnecting in 3s...")
					done = true
					break
				}

				if msg == nil || msg.Payload == "" {
					continue
				}

				var req struct {
					UUID           string `json:"uuid"`
					Coins          int64  `json:"coins"`
					SeasonalTokens int    `json:"seasonal_tokens"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &req); err != nil || req.UUID == "" {
					continue
				}

				errDb := s.db.SetBalance(ctx, req.UUID, req.Coins, req.SeasonalTokens)
				if errDb != nil {
					log.Printf("[PubSub Sync] Failed sync DB for %s: %v", req.UUID, errDb)
					continue
				}

				_ = s.leaderboard.UpdateLeaderboard(ctx, req.UUID, req.Coins)
				_ = s.cacheRepo.InvalidatePlayerEconomyCache(ctx, req.UUID)
				log.Printf("💾 Persisted updated balance to DB for %s (coins=%d, tokens=%d)", req.UUID, req.Coins, req.SeasonalTokens)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}
