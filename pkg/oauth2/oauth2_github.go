package oauth2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	githubAuthURL      = "https://github.com/login/oauth/authorize"
	githubTokenURL     = "https://github.com/login/oauth/access_token"
	githubUserURL      = "https://api.github.com/user"
	githubUserEmailURL = "https://api.github.com/user/emails"
)

// GitHubOAuth2Provider implements Provider interface for GitHub OAuth2
type GitHubOAuth2Provider struct {
	clientID     string
	clientSecret string
	redirectURL  string
	scopes       []string
	httpClient   *http.Client
}

type githubUser struct {
	ID        int64  `json:"id"`
	Login     string `json:"login"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	AvatarURL string `json:"avatar_url"`
}

type githubEmail struct {
	Email      string `json:"email"`
	Primary    bool   `json:"primary"`
	Verified   bool   `json:"verified"`
	Visibility string `json:"visibility"`
}

type githubTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

func NewGitHubOAuth2Provider(clientID, clientSecret, redirectURL string, scopes []string) *GitHubOAuth2Provider {
	if len(scopes) == 0 {
		scopes = []string{"read:user", "user:email"}
	}

	return &GitHubOAuth2Provider{
		clientID:     clientID,
		clientSecret: clientSecret,
		redirectURL:  redirectURL,
		scopes:       scopes,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (gh *GitHubOAuth2Provider) GetName() string {
	return "github"
}

func (gh *GitHubOAuth2Provider) GetAuthURL(state string, nonce string) string {
	params := url.Values{}
	params.Add("client_id", gh.clientID)
	params.Add("redirect_uri", gh.redirectURL)
	params.Add("scope", strings.Join(gh.scopes, " "))
	params.Add("state", state)
	// GitHub doesn't support nonce in OAuth2 flow, only in OIDC (which they don't support)
	// We ignore the nonce parameter for GitHub

	return githubAuthURL + "?" + params.Encode()
}

func (gh *GitHubOAuth2Provider) HandleCallback(ctx context.Context, code string, state string, nonce string) (*UserInfo, *TokenSet, error) {
	// Exchange code for token
	tokenResp, err := gh.exchangeCode(ctx, code)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to exchange code: %w", err)
	}

	// Get user info
	userInfo, err := gh.getUserInfo(ctx, tokenResp.AccessToken)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get user info: %w", err)
	}

	tokenSet := &TokenSet{
		AccessToken:  tokenResp.AccessToken,
		TokenType:    tokenResp.TokenType,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresAt:    time.Now().Add(24 * time.Hour), // GitHub tokens don't expire by default
	}

	return userInfo, tokenSet, nil
}

func (gh *GitHubOAuth2Provider) exchangeCode(ctx context.Context, code string) (*githubTokenResponse, error) {
	data := url.Values{}
	data.Set("client_id", gh.clientID)
	data.Set("client_secret", gh.clientSecret)
	data.Set("code", code)
	data.Set("redirect_uri", gh.redirectURL)

	req, err := http.NewRequestWithContext(ctx, "POST", githubTokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := gh.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp githubTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return nil, errors.New("no access token in response")
	}

	return &tokenResp, nil
}

func (gh *GitHubOAuth2Provider) getUserInfo(ctx context.Context, accessToken string) (*UserInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", githubUserURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create user info request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := gh.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get user info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("user info request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var ghUser githubUser
	if err := json.NewDecoder(resp.Body).Decode(&ghUser); err != nil {
		return nil, fmt.Errorf("failed to decode user info: %w", err)
	}

	email := ghUser.Email
	emailVerified := false

	// If email is not public, fetch from emails endpoint
	if email == "" {
		email, emailVerified, err = gh.getPrimaryEmail(ctx, accessToken)
		if err != nil {
			// Don't fail if we can't get email, some users might not have it
			email = ""
			emailVerified = false
		}
	}

	name := ghUser.Name
	if name == "" {
		name = ghUser.Login
	}

	return &UserInfo{
		ID:            fmt.Sprintf("%d", ghUser.ID),
		Email:         email,
		EmailVerified: emailVerified,
		Name:          name,
		Picture:       ghUser.AvatarURL,
		Provider:      "github",
		CreatedAt:     time.Now(),
	}, nil
}

func (gh *GitHubOAuth2Provider) getPrimaryEmail(ctx context.Context, accessToken string) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", githubUserEmailURL, nil)
	if err != nil {
		return "", false, err
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := gh.httpClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("failed to get emails with status %d", resp.StatusCode)
	}

	var emails []githubEmail
	if err := json.NewDecoder(resp.Body).Decode(&emails); err != nil {
		return "", false, err
	}

	// Find primary email
	for _, e := range emails {
		if e.Primary {
			return e.Email, e.Verified, nil
		}
	}

	// Fallback to first email if no primary
	if len(emails) > 0 {
		return emails[0].Email, emails[0].Verified, nil
	}

	return "", false, errors.New("no email found")
}
