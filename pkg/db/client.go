package db

import (
	"context"
	"database/sql"
	"fmt"
)

type TxFunc func(ctx context.Context, tx *sql.Tx) error

type SQLClient struct {
	db *sql.DB
}

func NewSQLClient(driver, dsn string) (*SQLClient, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}
	return &SQLClient{db: db}, nil
}

func (c *SQLClient) DB() *sql.DB {
	return c.db
}

func (c *SQLClient) WithTransaction(ctx context.Context, isolation sql.IsolationLevel, fn TxFunc) error {
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: isolation})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback error: %v, original error: %w", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit error: %w", err)
	}
	return nil
}

// ExecContext executes a query without returning rows (INSERT/UPDATE/DELETE)
func (c *SQLClient) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// QueryContext executes a query that returns multiple rows
func (c *SQLClient) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that returns a single row
func (c *SQLClient) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}
