package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisHandler struct {
	client   *redis.Client
	db       *Database
	workerID string
	wg       *sync.WaitGroup
}

func NewRedisHandler(addr string, password string, db *Database, workerID string, wg *sync.WaitGroup) *RedisHandler {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	return &RedisHandler{
		client:   client,
		db:       db,
		workerID: workerID,
		wg:       wg,
	}
}

func (r *RedisHandler) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// StartListening connects to Redis Stream minigames:events:match_results
func (r *RedisHandler) StartListening(ctx context.Context) {
	defer r.wg.Done()

	streamName := "minigames:events:match_results"
	groupName := "economy-service"

	err := r.client.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	log.Println("Listening for match results on Redis Stream:", streamName)

	r.wg.Add(1)
	go r.handlePendingMessages(ctx, streamName, groupName)

	r.wg.Add(1)
	go r.StartLevelUpPubSubListener(ctx)

	r.wg.Add(1)
	go r.StartClaimDailyCommandListener(ctx)

	r.wg.Add(1)
	go r.StartJoinListener(ctx)

	r.wg.Add(1)
	go r.StartSyncBalanceListener(ctx)

	for {
		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: r.workerID,
			Streams:  []string{streamName, ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()

		if err != nil {
			if err == context.Canceled {
				log.Println("Context canceled, exiting Redis stream loop")
				return
			}
			if err != redis.Nil {
				log.Printf("Error reading stream: %v", err)
				time.Sleep(2 * time.Second)
			}
			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}

		for _, stream := range streams {
			r.processBatch(ctx, streamName, groupName, stream.Messages)
		}
	}
}

func (r *RedisHandler) processBatch(ctx context.Context, streamName, groupName string, messages []redis.XMessage) {
	if len(messages) == 0 {
		return
	}

	start := time.Now()
	for _, msg := range messages {
		payloadRaw, ok := msg.Values["payload"]
		if !ok {
			r.client.XAck(ctx, streamName, groupName, msg.ID)
			continue
		}

		payloadStr, ok := payloadRaw.(string)
		if !ok {
			r.client.XAck(ctx, streamName, groupName, msg.ID)
			continue
		}

		var result MatchResult
		if err := json.Unmarshal([]byte(payloadStr), &result); err != nil {
			log.Printf("Failed to unmarshal match result payload: %v", err)
			r.client.XAck(ctx, streamName, groupName, msg.ID)
			continue
		}

		r.processMatchResult(ctx, result)
		r.client.XAck(ctx, streamName, groupName, msg.ID)
	}

	processDurationHistogram.Observe(time.Since(start).Seconds())
}

func (r *RedisHandler) handlePendingMessages(ctx context.Context, streamName, groupName string) {
	defer r.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pending, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
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
			if p.Idle > 30*time.Second {
				toClaim = append(toClaim, p.ID)
			}
		}

		if len(toClaim) > 0 {
			messages, err := r.client.XClaim(ctx, &redis.XClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: r.workerID,
				MinIdle:  30 * time.Second,
				Messages: toClaim,
			}).Result()

			if err == nil && len(messages) > 0 {
				log.Printf("Claimed %d pending messages for reprocessing", len(messages))
				r.processBatch(ctx, streamName, groupName, messages)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
	}
}

// StartLevelUpPubSubListener listens for levelup events from LevelingService
func (r *RedisHandler) StartLevelUpPubSubListener(ctx context.Context) {
	defer r.wg.Done()
	log.Println("Listening for LevelUp rewards on Pub/Sub: leveling:events:levelup")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := r.client.Subscribe(ctx, "leveling:events:levelup")
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
				log.Printf("[Redis PubSub] LevelUp listener connection lost (%v), reconnecting in 3 seconds...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				break
			}

			var event LevelUpEvent
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				log.Printf("Failed to parse levelup event: %v", err)
				continue
			}

			r.processLevelUpRewards(ctx, event)
		}
	}
}

func (r *RedisHandler) processMatchResult(ctx context.Context, result MatchResult) {
	for _, p := range result.Players {
		if p.UUID == "" {
			continue
		}

		earnedCoins := p.EarnedEconomy.Coins
		if earnedCoins <= 0 {
			earnedCoins = 20
		}

		if p.Status == "WINNER" {
			earnedCoins = int64(float64(earnedCoins) * 1.5)
		}

		idempKey := fmt.Sprintf("match-%s-%s", result.MatchID, p.UUID)
		updatedEco, err := r.db.AddBalance(ctx, p.UUID, "coins", earnedCoins, "MATCH_WIN", idempKey)
		if err != nil {
			log.Printf("Error adding match coins for %s: %v", p.UUID, err)
			continue
		}

		coinsAddedTotal.WithLabelValues("MATCH_WIN").Add(float64(earnedCoins))
		transactionsProcessedTotal.WithLabelValues("coins", "MATCH_WIN").Inc()

		r.CachePlayerEconomy(ctx, updatedEco)
		r.UpdateLeaderboard(ctx, p.UUID, updatedEco.Coins)
		r.PublishNotification(ctx, EconomyNotification{
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

func (r *RedisHandler) processLevelUpRewards(ctx context.Context, event LevelUpEvent) {
	totalCoinsReward := int64(0)
	for _, reward := range event.UnlockedRewards {
		totalCoinsReward += reward.Coins
	}

	if totalCoinsReward <= 0 {
		totalCoinsReward = int64(event.NewLevel * 100)
	}

	idempKey := fmt.Sprintf("levelup-%s-lvl%d", event.PlayerUUID, event.NewLevel)
	updatedEco, err := r.db.AddBalance(ctx, event.PlayerUUID, "coins", totalCoinsReward, "LEVEL_UP", idempKey)
	if err != nil {
		log.Printf("Error processing level up coins for %s: %v", event.PlayerUUID, err)
		return
	}

	log.Printf("💰 Awarded %d Level Up coins to %s (Level %d)", totalCoinsReward, event.PlayerUUID, event.NewLevel)

	coinsAddedTotal.WithLabelValues("LEVEL_UP").Add(float64(totalCoinsReward))
	transactionsProcessedTotal.WithLabelValues("coins", "LEVEL_UP").Inc()

	r.CachePlayerEconomy(ctx, updatedEco)
	r.UpdateLeaderboard(ctx, event.PlayerUUID, updatedEco.Coins)
	r.PublishNotification(ctx, EconomyNotification{
		UUID:                 event.PlayerUUID,
		Coins:                updatedEco.Coins,
		SeasonalTokens:       updatedEco.SeasonalTokens,
		ChangeCoins:          totalCoinsReward,
		ChangeSeasonalTokens: 0,
		Source:               "LEVEL_UP",
		Timestamp:            time.Now().Unix(),
	})
}

// CachePlayerEconomy caches profile into Redis Hash "player:economy:<uuid>"
func (r *RedisHandler) CachePlayerEconomy(ctx context.Context, p *PlayerEconomy) {
	key := fmt.Sprintf("player:economy:%s", p.PlayerUUID)
	data := map[string]interface{}{
		"coins":           p.Coins,
		"seasonal_tokens": p.SeasonalTokens,
		"login_streak":    p.LoginStreak,
		"can_claim_daily": p.CanClaimDaily,
		"updated_at":      p.UpdatedAt.Unix(),
	}
	r.client.HSet(ctx, key, data)
	r.client.Expire(ctx, key, 24*time.Hour)
}

// GetCachedPlayerEconomy retrieves profile from Redis cache
func (r *RedisHandler) GetCachedPlayerEconomy(ctx context.Context, playerUUID string) (*PlayerEconomy, error) {
	key := fmt.Sprintf("player:economy:%s", playerUUID)
	res, err := r.client.HGetAll(ctx, key).Result()
	if err != nil || len(res) == 0 {
		return nil, fmt.Errorf("cache miss")
	}

	coins, _ := strconv.ParseInt(res["coins"], 10, 64)
	tokens, _ := strconv.Atoi(res["seasonal_tokens"])
	streak, _ := strconv.Atoi(res["login_streak"])
	canClaim, _ := strconv.ParseBool(res["can_claim_daily"])

	return &PlayerEconomy{
		PlayerUUID:     playerUUID,
		Coins:          coins,
		SeasonalTokens: tokens,
		LoginStreak:    streak,
		CanClaimDaily:  canClaim,
	}, nil
}

// UpdateLeaderboard updates Redis Sorted Set "leaderboard:economy:coins"
func (r *RedisHandler) UpdateLeaderboard(ctx context.Context, playerUUID string, coins int64) {
	r.client.ZAdd(ctx, "leaderboard:economy:coins", redis.Z{
		Score:  float64(coins),
		Member: playerUUID,
	})
}

// PublishNotification sends live Pub/Sub message to "economy:notifications"
func (r *RedisHandler) PublishNotification(ctx context.Context, notif EconomyNotification) {
	payloadBytes, err := json.Marshal(notif)
	if err != nil {
		return
	}
	r.client.Publish(ctx, "economy:notifications", string(payloadBytes))
}

// StartSyncBalanceListener listens for external balance changes and persists them directly into PostgreSQL
func (r *RedisHandler) StartSyncBalanceListener(ctx context.Context) {
	defer r.wg.Done()
	log.Println("Listening for economy sync commands on Pub/Sub: economy:commands:sync, economy:notifications")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := r.client.Subscribe(ctx, "economy:commands:sync", "economy:notifications")
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
				log.Printf("[Redis PubSub] Sync listener connection lost (%v), reconnecting in 3 seconds...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				break
			}

			var req struct {
				UUID           string `json:"uuid"`
				Coins          int64  `json:"coins"`
				SeasonalTokens int    `json:"seasonal_tokens"`
			}
			if err := json.Unmarshal([]byte(msg.Payload), &req); err != nil || req.UUID == "" {
				continue
			}

			errDb := r.db.SetBalance(ctx, req.UUID, req.Coins, req.SeasonalTokens)
			if errDb != nil {
				log.Printf("Failed to sync balance to DB for %s: %v", req.UUID, errDb)
				continue
			}

			r.UpdateLeaderboard(ctx, req.UUID, req.Coins)
			log.Printf("💾 Persisted updated balance to DB for %s (coins=%d, tokens=%d)", req.UUID, req.Coins, req.SeasonalTokens)
		}
	}
}

// StartClaimDailyCommandListener listens for claim daily commands from Minecraft plugin over Pub/Sub
func (r *RedisHandler) StartClaimDailyCommandListener(ctx context.Context) {
	defer r.wg.Done()
	log.Println("Listening for claim daily commands on Pub/Sub: economy:commands:claim_daily")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := r.client.Subscribe(ctx, "economy:commands:claim_daily")
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
				log.Printf("[Redis PubSub] ClaimDaily listener connection lost (%v), reconnecting in 3 seconds...", err)
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

			res, err := r.db.ClaimDailyBonus(ctx, req.UUID)
			if err != nil {
				log.Printf("Failed to process daily claim via PubSub for %s: %v", req.UUID, err)
				continue
			}

			dailyClaimsTotal.WithLabelValues(strconv.Itoa(res.StreakDay)).Inc()
			coinsAddedTotal.WithLabelValues("DAILY_BONUS").Add(float64(res.CoinsAwarded))

			updatedEco, _ := r.db.GetPlayerEconomy(ctx, req.UUID)
			if updatedEco != nil {
				r.CachePlayerEconomy(ctx, updatedEco)
				r.UpdateLeaderboard(ctx, req.UUID, updatedEco.Coins)
			}

			r.PublishNotification(ctx, EconomyNotification{
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

// StartJoinListener listens for player_join events on Pub/Sub to pre-cache player economy profile in Redis
func (r *RedisHandler) StartJoinListener(ctx context.Context) {
	defer r.wg.Done()
	log.Println("Listening for player_join events on Pub/Sub: minigames:events:player_join")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pubsub := r.client.Subscribe(ctx, "minigames:events:player_join")
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
				log.Printf("[Redis PubSub] Economy join listener connection lost (%v), reconnecting in 3 seconds...", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				break
			}

			if msg != nil && msg.Payload != "" {
				uuid := msg.Payload
				profile, err := r.db.GetPlayerEconomy(ctx, uuid)
				if err == nil && profile != nil {
					r.CachePlayerEconomy(ctx, profile)
					r.UpdateLeaderboard(ctx, uuid, profile.Coins)
					log.Printf("[Economy] Synced economy profile for joining player %s (coins=%d)", uuid, profile.Coins)
				}
			}
		}
	}
}
