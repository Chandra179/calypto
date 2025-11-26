package oauth2

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionExpired  = errors.New("session expired")
)

// Session represents a user session with tokens
type Session struct {
	ID           string    `json:"id"`
	UserInfo     *UserInfo `json:"user_info"`
	TokenSet     *TokenSet `json:"token_set"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	LastAccessed time.Time `json:"last_accessed"`
}

// SessionStore interface for managing sessions
type SessionStore interface {
	Create(userInfo *UserInfo, tokenSet *TokenSet, ttl time.Duration) (*Session, error)
	Get(sessionID string) (*Session, error)
	Update(sessionID string, tokenSet *TokenSet) error
	Delete(sessionID string) error
	Cleanup()
}

// InMemorySessionStore implements SessionStore
type InMemorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	done     chan struct{}
	once     sync.Once
}

func NewInMemorySessionStore() *InMemorySessionStore {
	store := &InMemorySessionStore{
		sessions: make(map[string]*Session),
		done:     make(chan struct{}),
	}
	go store.cleanupRoutine()
	return store
}

func (s *InMemorySessionStore) Create(userInfo *UserInfo, tokenSet *TokenSet, ttl time.Duration) (*Session, error) {
	sessionID, err := GenerateRandomString(32)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	session := &Session{
		ID:           sessionID,
		UserInfo:     userInfo,
		TokenSet:     tokenSet,
		CreatedAt:    now,
		ExpiresAt:    now.Add(ttl),
		LastAccessed: now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = session

	return session, nil
}

func (s *InMemorySessionStore) Get(sessionID string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.sessions[sessionID]
	if !exists {
		return nil, ErrSessionNotFound
	}

	if time.Now().After(session.ExpiresAt) {
		delete(s.sessions, sessionID)
		return nil, ErrSessionExpired
	}

	// Update last accessed time
	session.LastAccessed = time.Now()

	return session, nil
}

func (s *InMemorySessionStore) Update(sessionID string, tokenSet *TokenSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, exists := s.sessions[sessionID]
	if !exists {
		return ErrSessionNotFound
	}

	if time.Now().After(session.ExpiresAt) {
		delete(s.sessions, sessionID)
		return ErrSessionExpired
	}

	session.TokenSet = tokenSet
	session.LastAccessed = time.Now()

	return nil
}

func (s *InMemorySessionStore) Delete(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
	return nil
}

func (s *InMemorySessionStore) Cleanup() {
	s.once.Do(func() {
		close(s.done)
	})
}

func (s *InMemorySessionStore) cleanupRoutine() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.removeExpired()
		case <-s.done:
			return
		}
	}
}

func (s *InMemorySessionStore) removeExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, session := range s.sessions {
		if now.After(session.ExpiresAt) {
			delete(s.sessions, id)
		}
	}
}
