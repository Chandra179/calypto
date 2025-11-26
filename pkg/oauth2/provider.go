package oauth2

import (
	"context"
	"time"
)

// Provider defines the base interface for authentication providers
type Provider interface {
	GetName() string
	GetAuthURL(state string, nonce string) string
	HandleCallback(ctx context.Context, code string, state string, nonce string) (*UserInfo, *TokenSet, error)
}

// UserInfo represents unified user information across providers
type UserInfo struct {
	ID            string    `json:"id"`
	Email         string    `json:"email"`
	EmailVerified bool      `json:"email_verified"`
	Name          string    `json:"name"`
	Picture       string    `json:"picture"`
	Provider      string    `json:"provider"`
	CreatedAt     time.Time `json:"created_at"`
}

// TokenSet represents the complete token response from providers
type TokenSet struct {
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	IDToken      string    `json:"id_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
}
