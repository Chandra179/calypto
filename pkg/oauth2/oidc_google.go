package oauth2

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// GoogleOIDCProvider implements Provider interface using OIDC
type GoogleOIDCProvider struct {
	config       *oauth2.Config
	provider     *oidc.Provider
	verifier     *oidc.IDTokenVerifier
	providerName string
}

// NewGoogleOIDCProvider creates a new Google OIDC provider
func NewGoogleOIDCProvider(ctx context.Context, clientID, clientSecret, redirectURL string, scopes []string) (*GoogleOIDCProvider, error) {
	provider, err := oidc.NewProvider(ctx, "https://accounts.google.com")
	if err != nil {
		return nil, fmt.Errorf("failed to create OIDC provider: %w", err)
	}

	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "profile", "email"}
	}

	config := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       scopes,
	}

	verifier := provider.Verifier(&oidc.Config{
		ClientID: clientID,
	})

	return &GoogleOIDCProvider{
		config:       config,
		provider:     provider,
		verifier:     verifier,
		providerName: "google",
	}, nil
}

func (g *GoogleOIDCProvider) GetName() string {
	return g.providerName
}

func (g *GoogleOIDCProvider) GetAuthURL(state string, nonce string) string {
	opts := []oauth2.AuthCodeOption{
		oauth2.AccessTypeOffline,
		oauth2.ApprovalForce,
		oidc.Nonce(nonce),
	}
	return g.config.AuthCodeURL(state, opts...)
}

func (g *GoogleOIDCProvider) HandleCallback(ctx context.Context, code string, state string, nonce string) (*UserInfo, *TokenSet, error) {
	oauth2Token, err := g.config.Exchange(ctx, code)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exchange code: %w", err)
	}

	// Extract ID token
	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		return nil, nil, fmt.Errorf("no id_token in response")
	}

	// Verify ID token
	idToken, err := g.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to verify ID token: %w", err)
	}

	// Verify nonce
	if idToken.Nonce != nonce {
		return nil, nil, fmt.Errorf("nonce mismatch")
	}

	// Extract claims
	var claims struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
	}

	if err := idToken.Claims(&claims); err != nil {
		return nil, nil, fmt.Errorf("failed to parse claims: %w", err)
	}

	userInfo := &UserInfo{
		ID:            idToken.Subject,
		Email:         claims.Email,
		EmailVerified: claims.EmailVerified,
		Name:          claims.Name,
		Picture:       claims.Picture,
		Provider:      g.providerName,
		CreatedAt:     time.Now(),
	}

	tokenSet := &TokenSet{
		AccessToken:  oauth2Token.AccessToken,
		TokenType:    oauth2Token.TokenType,
		RefreshToken: oauth2Token.RefreshToken,
		IDToken:      rawIDToken,
		ExpiresAt:    oauth2Token.Expiry,
	}

	return userInfo, tokenSet, nil
}

// RefreshToken refreshes the access token using refresh token
func (g *GoogleOIDCProvider) RefreshToken(ctx context.Context, refreshToken string) (*TokenSet, error) {
	tokenSource := g.config.TokenSource(ctx, &oauth2.Token{
		RefreshToken: refreshToken,
	})

	newToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	tokenSet := &TokenSet{
		AccessToken:  newToken.AccessToken,
		TokenType:    newToken.TokenType,
		RefreshToken: newToken.RefreshToken,
		ExpiresAt:    newToken.Expiry,
	}

	if idToken, ok := newToken.Extra("id_token").(string); ok {
		tokenSet.IDToken = idToken
	}

	return tokenSet, nil
}
