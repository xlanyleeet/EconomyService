package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"economy-service/internal/domain"
	"economy-service/internal/event/pubsub"
	"economy-service/internal/metrics"
	"economy-service/internal/repository/postgres"
	"economy-service/internal/repository/redis"

	goredis "github.com/redis/go-redis/v9"
)

type MatchConsumer struct {
	redisClient *redis.Client
	db          *postgres.Database
	cacheRepo   *redis.CacheRepository
	leaderboard *redis.LeaderboardRepository
	publisher   *pubsub.Publisher
	workerID    string
	wg          *sync.WaitGroup
}

func NewMatchConsumer(
	redisClient *redis.Client,
	db *postgres.Database,
	cacheRepo *redis.CacheRepository,
	leaderboard *redis.LeaderboardRepository,
	publisher *pubsub.Publisher,
	workerID string,
	wg *sync.WaitGroup,
) *MatchConsumer {
	return &MatchConsumer{
		redisClient: redisClient,
		db:          db,
		cacheRepo:   cacheRepo,
		leaderboard: leaderboard,
		publisher:   publisher,
		workerID:    workerID,
		wg:          wg,
	}
}

// StartListening connects to Redis Stream minigames:events:match_results
func (c *MatchConsumer) StartListening(ctx context.Context) {
	defer c.wg.Done()

	streamName := "minigames:events:match_results"
	groupName := "economy-service"

	err := c.redisClient.RawClient().XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("[MatchConsumer] Failed to create consumer group: %v", err)
	}

	log.Println("[MatchConsumer] Listening for match results on Redis Stream:", streamName)

	c.wg.Add(1)
	go c.handlePendingMessages(ctx, streamName, groupName)

	for {
		streams, err := c.redisClient.RawClient().XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    groupName,
			Consumer: c.workerID,
			Streams:  []string{streamName, ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()

		if err != nil {
			if err == context.Canceled {
				log.Println("[MatchConsumer] Context canceled, exiting stream loop")
				return
			}
			if err != goredis.Nil {
				log.Printf("[MatchConsumer] Error reading stream: %v", err)
				time.Sleep(2 * time.Second)
			}
			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}

		for _, s := range streams {
			c.processBatch(ctx, streamName, groupName, s.Messages)
		}
	}
}

func (c *MatchConsumer) processBatch(ctx context.Context, streamName, groupName string, messages []goredis.XMessage) {
	if len(messages) == 0 {
		return
	}

	start := time.Now()
	for _, msg := range messages {
		payloadRaw, ok := msg.Values["payload"]
		if !ok {
			_ = c.redisClient.RawClient().XAck(ctx, streamName, groupName, msg.ID)
			continue
		}

		payloadStr, ok := payloadRaw.(string)
		if !ok {
			_ = c.redisClient.RawClient().XAck(ctx, streamName, groupName, msg.ID)
			continue
		}

		var result domain.MatchResult
		if err := json.Unmarshal([]byte(payloadStr), &result); err != nil {
			log.Printf("[MatchConsumer] Failed to unmarshal match payload: %v", err)
			_ = c.redisClient.RawClient().XAck(ctx, streamName, groupName, msg.ID)
			continue
		}

		c.processMatchResult(ctx, result)
		_ = c.redisClient.RawClient().XAck(ctx, streamName, groupName, msg.ID)
	}

	metrics.ProcessDurationHistogram.Observe(time.Since(start).Seconds())
}

func (c *MatchConsumer) handlePendingMessages(ctx context.Context, streamName, groupName string) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pending, err := c.redisClient.RawClient().XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream: streamName,
			Group:  groupName,
			Start:  "-",
			End:    "+",
			Count:  20,
		}).Result()

		if err != nil || len(pending) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}

		var toClaim []string
		for _, p := range pending {
			if p.RetryCount > 5 {
				log.Printf("[MatchConsumer] Acking poison pill message %s after %d retries", p.ID, p.RetryCount)
				_ = c.redisClient.RawClient().XAck(ctx, streamName, groupName, p.ID).Err()
				continue
			}
			if p.Idle > 30*time.Second {
				toClaim = append(toClaim, p.ID)
			}
		}

		if len(toClaim) > 0 {
			messages, err := c.redisClient.RawClient().XClaim(ctx, &goredis.XClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: c.workerID,
				MinIdle:  30 * time.Second,
				Messages: toClaim,
			}).Result()

			if err == nil && len(messages) > 0 {
				log.Printf("[MatchConsumer] Claimed %d pending messages for reprocessing", len(messages))
				c.processBatch(ctx, streamName, groupName, messages)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
	}
}

func (c *MatchConsumer) processMatchResult(ctx context.Context, result domain.MatchResult) {
	for _, p := range result.Players {
		if p.UUID == "" {
			continue
		}

		earnedCoins := p.EarnedEconomy.Coins
		if earnedCoins < 0 {
			earnedCoins = 0
		}
		if earnedCoins == 0 {
			earnedCoins = 20
		}

		// Boundary sanity check: ensure rewards cannot exceed maximum allowable limit per match
		if earnedCoins > 1000000 {
			earnedCoins = 1000000
		}

		idempKey := fmt.Sprintf("match-%s-%s", result.MatchID, p.UUID)
		updatedEco, err := c.db.AddBalance(ctx, p.UUID, "coins", earnedCoins, "MATCH_WIN", idempKey)
		if err != nil {
			log.Printf("[MatchConsumer] Error adding match coins for %s: %v", p.UUID, err)
			continue
		}

		metrics.CoinsAddedTotal.WithLabelValues("MATCH_WIN").Add(float64(earnedCoins))
		metrics.TransactionsProcessedTotal.WithLabelValues("coins", "MATCH_WIN").Inc()

		_ = c.cacheRepo.CachePlayerEconomy(ctx, updatedEco)
		_ = c.leaderboard.UpdateLeaderboard(ctx, p.UUID, updatedEco.Coins)
		_ = c.publisher.PublishNotification(ctx, domain.EconomyNotification{
			UUID:                 p.UUID,
			Coins:                updatedEco.Coins,
			SeasonalTokens:       updatedEco.SeasonalTokens,
			ChangeCoins:          earnedCoins,
			ChangeSeasonalTokens: 0,
			Source:               "MATCH_WIN",
			Timestamp:            time.Now().Unix(),
		})
	}
}
