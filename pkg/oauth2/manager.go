package oauth2

import (
	"calypto/cfg"
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrProviderNotFound = errors.New("provider not found")
	ErrInvalidState     = errors.New("invalid state")
)

// Manager manages OAuth2/OIDC providers and authentication flow
type Manager struct {
	providers      map[string]Provider
	stateStorage   StateStorage
	sessionStore   SessionStore
	stateTimeout   time.Duration
	sessionTimeout time.Duration
}

// NewManager creates a new manager with providers from configuration
func NewManager(ctx context.Context, cfg *cfg.Oauth2Config) (*Manager, error) {
	mgr := &Manager{
		providers:      make(map[string]Provider),
		stateStorage:   NewInMemoryStorage(),
		sessionStore:   NewInMemorySessionStore(),
		stateTimeout:   10 * time.Minute,
		sessionTimeout: 24 * time.Hour,
	}

	// Initialize Google OIDC provider
	if cfg.GoogleClientID != "" && cfg.GoogleClientSecret != "" {
		googleProvider, err := NewGoogleOIDCProvider(
			ctx,
			cfg.GoogleClientID,
			cfg.GoogleClientSecret,
			cfg.GoogleRedirectUrl,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google provider: %w", err)
		}
		mgr.RegisterProvider(googleProvider)
	}

	// Initialize GitHub OAuth2 provider
	if cfg.GithubClientID != "" && cfg.GithubClientSecret != "" {
		githubProvider := NewGitHubOAuth2Provider(
			cfg.GithubClientID,
			cfg.GithubClientSecret,
			cfg.GithubRedirectUrl,
			nil,
		)
		mgr.RegisterProvider(githubProvider)
	}

	return mgr, nil
}

// RegisterProvider registers a new authentication provider
func (m *Manager) RegisterProvider(provider Provider) {
	m.providers[provider.GetName()] = provider
}

// GetAuthURL generates authorization URL with state and nonce
func (m *Manager) GetAuthURL(providerName string) (string, error) {
	provider, exists := m.providers[providerName]
	if !exists {
		return "", ErrProviderNotFound
	}

	// Generate state
	state, err := GenerateRandomString(32)
	if err != nil {
		return "", fmt.Errorf("failed to generate state: %w", err)
	}

	// Generate nonce
	nonce, err := GenerateRandomString(32)
	if err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Store state and nonce
	expiresAt := time.Now().Add(m.stateTimeout)
	if err := m.stateStorage.SaveState(state, nonce, expiresAt); err != nil {
		return "", fmt.Errorf("failed to save state: %w", err)
	}

	return provider.GetAuthURL(state, nonce), nil
}

// HandleCallback handles OAuth2/OIDC callback and creates a session
func (m *Manager) HandleCallback(ctx context.Context, providerName, code, state string) (string, *UserInfo, error) {
	provider, exists := m.providers[providerName]
	if !exists {
		return "", nil, ErrProviderNotFound
	}

	// Get and validate nonce from storage
	nonce, err := m.stateStorage.(*InMemoryStorage).GetNonce(state)
	if err != nil {
		return "", nil, fmt.Errorf("invalid state: %w", err)
	}

	// Delete state after retrieval (one-time use)
	defer m.stateStorage.DeleteState(state)

	// Handle callback through provider
	userInfo, tokenSet, err := provider.HandleCallback(ctx, code, state, nonce)
	if err != nil {
		return "", nil, fmt.Errorf("callback failed: %w", err)
	}

	// Create session
	session, err := m.sessionStore.Create(userInfo, tokenSet, m.sessionTimeout)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create session: %w", err)
	}

	return session.ID, userInfo, nil
}

// GetSession retrieves a session by ID
func (m *Manager) GetSession(sessionID string) (*Session, error) {
	return m.sessionStore.Get(sessionID)
}

// RefreshSession refreshes tokens for a session (only for OIDC providers)
func (m *Manager) RefreshSession(ctx context.Context, sessionID string) error {
	session, err := m.sessionStore.Get(sessionID)
	if err != nil {
		return err
	}

	if session.TokenSet.RefreshToken == "" {
		return errors.New("no refresh token available")
	}

	// Only Google OIDC provider supports refresh
	provider, exists := m.providers[session.UserInfo.Provider]
	if !exists {
		return ErrProviderNotFound
	}

	googleProvider, ok := provider.(*GoogleOIDCProvider)
	if !ok {
		return errors.New("provider does not support token refresh")
	}

	newTokenSet, err := googleProvider.RefreshToken(ctx, session.TokenSet.RefreshToken)
	if err != nil {
		return fmt.Errorf("failed to refresh token: %w", err)
	}

	// Update session with new tokens
	return m.sessionStore.Update(sessionID, newTokenSet)
}

// DeleteSession deletes a session (logout)
func (m *Manager) DeleteSession(sessionID string) error {
	return m.sessionStore.Delete(sessionID)
}

// Cleanup cleans up storage resources
func (m *Manager) Cleanup() {
	m.stateStorage.Cleanup()
	m.sessionStore.Cleanup()
}
