package passkey

import (
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
)

// User represents a user with passkey credentials
type User struct {
	ID          []byte
	Username    string
	Credentials []Credential
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Credential extends webauthn.Credential with metadata
type Credential struct {
	webauthn.Credential
	Nickname    string
	CreatedAt   time.Time
	LastUsedAt  time.Time
	Transports  []string
	BackupState bool
}

func (u *User) WebAuthnID() []byte {
	return u.ID
}

func (u *User) WebAuthnName() string {
	return u.Username
}

func (u *User) WebAuthnDisplayName() string {
	return u.Username
}

func (u *User) WebAuthnIcon() string {
	return ""
}

func (u *User) WebAuthnCredentials() []webauthn.Credential {
	credentials := make([]webauthn.Credential, len(u.Credentials))
	for i, c := range u.Credentials {
		credentials[i] = c.Credential
	}
	return credentials
}

func (u *User) AddCredential(cred webauthn.Credential, nickname string, transports []string) {
	u.Credentials = append(u.Credentials, Credential{
		Credential:  cred,
		Nickname:    nickname,
		CreatedAt:   time.Now(),
		LastUsedAt:  time.Now(),
		Transports:  transports,
		BackupState: cred.Flags.BackupState,
	})
	u.UpdatedAt = time.Now()
}

func (u *User) UpdateCredential(cred webauthn.Credential) {
	for i, c := range u.Credentials {
		if string(c.ID) == string(cred.ID) {
			u.Credentials[i].Credential = cred
			u.Credentials[i].LastUsedAt = time.Now()
			u.Credentials[i].BackupState = cred.Flags.BackupState
			u.UpdatedAt = time.Now()
			return
		}
	}
}

func (u *User) DeleteCredential(credentialID []byte) bool {
	for i, c := range u.Credentials {
		if string(c.ID) == string(credentialID) {
			u.Credentials = append(u.Credentials[:i], u.Credentials[i+1:]...)
			u.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

func (u *User) RenameCredential(credentialID []byte, nickname string) bool {
	for i, c := range u.Credentials {
		if string(c.ID) == string(credentialID) {
			u.Credentials[i].Nickname = nickname
			u.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

func (u *User) GetCredential(credentialID []byte) *Credential {
	for i, c := range u.Credentials {
		if string(c.ID) == string(credentialID) {
			return &u.Credentials[i]
		}
	}
	return nil
}

// Storage interface for user and session data
type Storage interface {
	GetUser(username string) (*User, error)
	GetUserByID(id []byte) (*User, error)
	CreateUser(username string) (*User, error)
	UpdateUser(user *User) error
	SaveSession(sessionID string, data *webauthn.SessionData) error
	GetSession(sessionID string) (*webauthn.SessionData, error)
	DeleteSession(sessionID string) error
	CleanupExpiredSessions() int
}

// InMemoryStorage stores users and sessions in memory
type InMemoryStorage struct {
	users    map[string]*User
	userByID map[string]*User
	sessions map[string]*sessionWithExpiry
	mu       sync.RWMutex
}

type sessionWithExpiry struct {
	data      *webauthn.SessionData
	expiresAt time.Time
}

func NewInMemoryStorage() *InMemoryStorage {
	storage := &InMemoryStorage{
		users:    make(map[string]*User),
		userByID: make(map[string]*User),
		sessions: make(map[string]*sessionWithExpiry),
	}

	// Start background cleanup goroutine
	go storage.cleanupRoutine()

	return storage
}

func (s *InMemoryStorage) cleanupRoutine() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.CleanupExpiredSessions()
	}
}

func (s *InMemoryStorage) GetUser(username string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	user, exists := s.users[username]
	if !exists {
		return nil, errors.New("user not found")
	}
	return user, nil
}

func (s *InMemoryStorage) GetUserByID(id []byte) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	user, exists := s.userByID[string(id)]
	if !exists {
		return nil, errors.New("user not found")
	}
	return user, nil
}

func (s *InMemoryStorage) CreateUser(username string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; exists {
		return nil, errors.New("user already exists")
	}

	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}

	now := time.Now()
	user := &User{
		ID:          id,
		Username:    username,
		Credentials: []Credential{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	s.users[username] = user
	s.userByID[string(id)] = user
	return user, nil
}

func (s *InMemoryStorage) UpdateUser(user *User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	user.UpdatedAt = time.Now()
	s.users[user.Username] = user
	s.userByID[string(user.ID)] = user
	return nil
}

func (s *InMemoryStorage) SaveSession(sessionID string, data *webauthn.SessionData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = &sessionWithExpiry{
		data:      data,
		expiresAt: time.Now().Add(5 * time.Minute),
	}
	return nil
}

func (s *InMemoryStorage) GetSession(sessionID string) (*webauthn.SessionData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, exists := s.sessions[sessionID]
	if !exists {
		return nil, errors.New("session not found")
	}
	if time.Now().After(session.expiresAt) {
		return nil, errors.New("session expired")
	}
	return session.data, nil
}

func (s *InMemoryStorage) DeleteSession(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
	return nil
}

func (s *InMemoryStorage) CleanupExpiredSessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	now := time.Now()
	for id, session := range s.sessions {
		if now.After(session.expiresAt) {
			delete(s.sessions, id)
			count++
		}
	}
	return count
}
