package wallet

import (
	"calypto/pkg/db"
	"calypto/pkg/idgen"
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Wallet holds the dependencies for the finance service.
// We will add the DB connection here later (e.g., DB *sql.DB).
type Wallet struct {
	IdGen idgen.Generator
	Db    *db.SQLClient
}

type TransactionRequest struct {
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
	FromAccountID  int64  `json:"from_account_id" binding:"required"`
	ToAccountID    int64  `json:"to_account_id" binding:"required"`
	Amount         int64  `json:"amount" binding:"required,gt=0"` // Positive integers only
	Reference      string `json:"reference"`
}

type CreateAccountRequest struct {
	UserID   string `json:"user_id" binding:"required"` // UUID string
	Currency string `json:"currency" binding:"required,len=3"`
}

// NewHandler creates a new instance of the Wallet service with dependencies injected.
func NewHandler(r *gin.Engine, idGen idgen.Generator, db *db.SQLClient) {
	w := &Wallet{IdGen: idGen, Db: db}
	v1 := r.Group("/api/v1")
	{
		// Account Routes
		v1.POST("/accounts", w.CreateAccount)
		v1.GET("/accounts/:id/balance", w.GetBalance)

		// Transaction Routes
		v1.POST("/transactions", w.PostTransaction)

		// Ledger History
		v1.GET("/transactions/:account_id", w.GetTransactionHistory)
	}
}

func (w *Wallet) CreateAccount(c *gin.Context) {
	var req CreateAccountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	accountID := w.IdGen.GenerateID()

	query := `
		INSERT INTO accounts (id, user_id, currency, balance, version, last_updated)
		VALUES ($1, $2, $3, 0, 1, NOW())
	`

	_, err := w.Db.DB().ExecContext(c.Request.Context(), query, accountID, req.UserID, req.Currency)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to create account: %v", err)})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"account_id": fmt.Sprintf("%d", accountID),
		"currency":   req.Currency,
		"balance":    0,
	})
}

func (w *Wallet) GetBalance(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "GetBalance not implemented"})
}

func (w *Wallet) PostTransaction(c *gin.Context) {
	var req TransactionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	txID := w.IdGen.GenerateID()
	postingID1 := w.IdGen.GenerateID()
	postingID2 := w.IdGen.GenerateID()

	err := w.Db.WithTransaction(c.Request.Context(), sql.LevelReadCommitted, func(ctx context.Context, tx *sql.Tx) error {

		// Idempotency Check
		// If this key exists, return the existing transaction or error out.
		var existingID int64
		err := tx.QueryRowContext(ctx, "SELECT id FROM transactions WHERE idempotency_key = $1", req.IdempotencyKey).Scan(&existingID)
		if err == nil {
			return fmt.Errorf("idempotency conflict: transaction already processed with ID %d", existingID)
		}

		// Debit Sender (Optimistic Locking)
		// We decrement balance AND check version in one go.
		// Returns 0 rows affected if balance is insufficient OR version changed.
		res, err := tx.ExecContext(ctx, `
            UPDATE accounts 
            SET balance = balance - $1, version = version + 1, last_updated = NOW()
            WHERE id = $2 AND balance >= $1
        `, req.Amount, req.FromAccountID)
		if err != nil {
			return fmt.Errorf("failed to debit account: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			// In a real app, you must distinguish between "insufficient funds" and "version mismatch"
			// usually by doing a SELECT first, but for high-throughput, the single query is faster.
			return fmt.Errorf("transaction failed: insufficient funds or concurrent modification")
		}

		// Credit Receiver
		// We do not check balance for the receiver, but we still update version.
		if _, err := tx.ExecContext(ctx, `
            UPDATE accounts 
            SET balance = balance + $1, version = version + 1, last_updated = NOW()
            WHERE id = $2
        `, req.Amount, req.ToAccountID); err != nil {
			return fmt.Errorf("failed to credit account: %w", err)
		}

		// Insert Transaction Record (The Intent)
		if _, err := tx.ExecContext(ctx, `
            INSERT INTO transactions (id, idempotency_key, reference, status, created_at)
            VALUES ($1, $2, $3, 'POSTED', NOW())
        `, txID, req.IdempotencyKey, req.Reference); err != nil {
			return fmt.Errorf("failed to insert transaction: %w", err)
		}

		// Insert Ledger Postings (The Immutable History)
		// Debit Leg
		if _, err := tx.ExecContext(ctx, `
            INSERT INTO ledger_postings (id, transaction_id, account_id, amount, direction)
            VALUES ($1, $2, $3, $4, 'DEBIT')
        `, postingID1, txID, req.FromAccountID, -req.Amount); err != nil {
			return err
		}
		// Credit Leg
		if _, err := tx.ExecContext(ctx, `
            INSERT INTO ledger_postings (id, transaction_id, account_id, amount, direction)
            VALUES ($1, $2, $3, $4, 'CREDIT')
        `, postingID2, txID, req.ToAccountID, req.Amount); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"transaction_id": fmt.Sprintf("%d", txID), "status": "POSTED"})
}

func (w *Wallet) GetTransactionHistory(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"message": "History not implemented"})
}
