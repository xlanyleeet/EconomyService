package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Database struct {
	pool *pgxpool.Pool
}

func NewDatabase(ctx context.Context, connString string) (*Database, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database config: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	db := &Database{pool: pool}
	if err := db.initTables(ctx); err != nil {
		return nil, fmt.Errorf("failed to init database tables: %w", err)
	}

	return db, nil
}

func (db *Database) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

func (db *Database) Pool() *pgxpool.Pool {
	return db.pool
}

func (db *Database) initTables(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS player_economy (
		player_uuid UUID PRIMARY KEY,
		coins BIGINT NOT NULL DEFAULT 0,
		seasonal_tokens INT NOT NULL DEFAULT 0,
		login_streak INT NOT NULL DEFAULT 0,
		last_login_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		last_daily_claim_at TIMESTAMP WITH TIME ZONE,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS economy_transactions (
		transaction_id UUID PRIMARY KEY,
		player_uuid UUID NOT NULL,
		currency VARCHAR(32) NOT NULL,
		amount BIGINT NOT NULL,
		balance_after BIGINT NOT NULL,
		source VARCHAR(64) NOT NULL,
		idempotency_key VARCHAR(128) UNIQUE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_economy_transactions_player ON economy_transactions(player_uuid, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_player_economy_coins ON player_economy(coins DESC);
	`

	_, err := db.pool.Exec(ctx, schema)
	return err
}
