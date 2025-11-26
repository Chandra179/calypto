package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"gosdk/cfg"
	"gosdk/pkg/db"
)

func main() {
	// ============
	// Load config
	// ============
	config, errCfg := cfg.Load()
	if errCfg != nil {
		log.Fatal(errCfg)
	}

	// ============
	// Build Postgres DSN from config
	// ============
	pg := config.Postgres
	pgDSN := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		pg.User,
		pg.Password,
		pg.Host,
		pg.Port,
		pg.DBName,
		pg.SSLMode,
	)

	// ============
	// Init DB client
	// ============
	client, err := db.NewSQLClient("postgres", pgDSN)
	if err != nil {
		log.Fatal(err)
	}

	// ============
	// Example transaction
	// ============
	err = client.WithTransaction(context.Background(), sql.LevelSerializable,
		func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO users(id, name) VALUES($1, $2)", 1, "Alice")
			if err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		fmt.Println("Transaction failed:", err)
	} else {
		fmt.Println("Transaction committed successfully")
	}
}
