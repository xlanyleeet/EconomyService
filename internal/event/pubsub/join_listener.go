package pubsub

import (
	"context"
	"log"
	"time"
)

func (s *Subscriber) startJoinListener(ctx context.Context) {
	defer s.wg.Done()
	log.Println("[PubSub] Listening for player_join events: minigames:events:player_join")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := s.redisClient.RawClient().Subscribe(ctx, "minigames:events:player_join")
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
				log.Printf("[PubSub Join] Connection lost (%v), reconnecting in 3s...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				break
			}

			if msg != nil && msg.Payload != "" {
				uuidStr := msg.Payload
				profile, err := s.db.GetPlayerEconomy(ctx, uuidStr)
				if err == nil && profile != nil {
					_ = s.cacheRepo.CachePlayerEconomy(ctx, profile)
					_ = s.leaderboard.UpdateLeaderboard(ctx, uuidStr, profile.Coins)
					log.Printf("[PubSub Join] Synced profile for joining player %s (coins=%d)", uuidStr, profile.Coins)
				}
			}
		}
	}
}
