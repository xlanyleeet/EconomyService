package pubsub

import (
	"context"
	"encoding/json"

	"economy-service/internal/domain"
	"economy-service/internal/repository/redis"
)

type Publisher struct {
	redisClient *redis.Client
}

func NewPublisher(redisClient *redis.Client) *Publisher {
	return &Publisher{redisClient: redisClient}
}

// PublishNotification sends live Pub/Sub message to "economy:notifications"
func (p *Publisher) PublishNotification(ctx context.Context, notif domain.EconomyNotification) error {
	payloadBytes, err := json.Marshal(notif)
	if err != nil {
		return err
	}
	return p.redisClient.RawClient().Publish(ctx, "economy:notifications", string(payloadBytes)).Err()
}
