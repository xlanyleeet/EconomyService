package redis

import (
	"github.com/redis/go-redis/v9"
)

type Client struct {
	cli *redis.Client
}

func NewClient(addr string, password string) *Client {
	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	return &Client{cli: cli}
}

func (c *Client) Close() error {
	if c.cli != nil {
		return c.cli.Close()
	}
	return nil
}

func (c *Client) RawClient() *redis.Client {
	return c.cli
}
