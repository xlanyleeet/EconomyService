package postgres

import (
	"context"

	"economy-service/internal/domain"
)

// GetTopPlayers retrieves top players by coins for leaderboard rebuilding
func (db *Database) GetTopPlayers(ctx context.Context, limit int) ([]domain.PlayerEconomy, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT player_uuid, coins, seasonal_tokens, login_streak, updated_at
		FROM player_economy
		ORDER BY coins DESC
		LIMIT $1
	`
	rows, err := db.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var players []domain.PlayerEconomy
	for rows.Next() {
		var p domain.PlayerEconomy
		if err := rows.Scan(&p.PlayerUUID, &p.Coins, &p.SeasonalTokens, &p.LoginStreak, &p.UpdatedAt); err != nil {
			return nil, err
		}
		players = append(players, p)
	}

	return players, nil
}
