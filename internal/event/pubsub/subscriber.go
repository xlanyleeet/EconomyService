package pubsub

import (
	"context"
	"sync"

	"economy-service/internal/repository/postgres"
	"economy-service/internal/repository/redis"
)

type Subscriber struct {
	redisClient *redis.Client
	db          *postgres.Database
	cacheRepo   *redis.CacheRepository
	leaderboard *redis.LeaderboardRepository
	publisher   *Publisher
	wg          *sync.WaitGroup
}

func NewSubscriber(
	redisClient *redis.Client,
	db *postgres.Database,
	cacheRepo *redis.CacheRepository,
	leaderboard *redis.LeaderboardRepository,
	publisher *Publisher,
	wg *sync.WaitGroup,
) *Subscriber {
	return &Subscriber{
		redisClient: redisClient,
		db:          db,
		cacheRepo:   cacheRepo,
		leaderboard: leaderboard,
		publisher:   publisher,
		wg:          wg,
	}
}

// StartAllListeners launches all Pub/Sub subscribers in background goroutines
func (s *Subscriber) StartAllListeners(ctx context.Context) {
	s.wg.Add(1)
	go s.startLevelUpListener(ctx)

	s.wg.Add(1)
	go s.startClaimDailyListener(ctx)

	s.wg.Add(1)
	go s.startSyncBalanceListener(ctx)

	s.wg.Add(1)
	go s.startJoinListener(ctx)
}
