package oauth2

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

const (
	sessionCookieName = "session_id"
	cookieMaxAge      = 86400 // 24 hours
)

// GoogleAuthHandler starts the Google OAuth2 flow
// @Summary Start Google OAuth2 login
// @Description Redirects user to Google OAuth2 login page
// @Tags oauth2
// @Produce json
// @Success 302 {string} string "Redirect"
// @Router /auth/google [get]
func GoogleAuthHandler(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authURL, err := manager.GetAuthURL("google")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.Redirect(http.StatusTemporaryRedirect, authURL)
	}
}

// GithubAuthHandler starts the GitHub OAuth2 flow
// @Summary Start GitHub OAuth2 login
// @Description Redirects user to GitHub OAuth2 login page
// @Tags oauth2
// @Produce json
// @Success 302 {string} string "Redirect"
// @Router /auth/github [get]
func GithubAuthHandler(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authURL, err := manager.GetAuthURL("github")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.Redirect(http.StatusTemporaryRedirect, authURL)
	}
}

// GoogleCallbackHandler handles Google OAuth2 callback
// @Summary Google OAuth2 callback
// @Description Handles Google OAuth2 callback and creates session
// @Tags oauth2
// @Produce json
// @Param code query string true "OAuth2 code"
// @Param state query string true "OAuth2 state"
// @Success 200 {object} map[string]string "Authenticated"
// @Failure 401 {object} map[string]string "Unauthorized"
// @Router /auth/callback/google [get]
func GoogleCallbackHandler(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		code := c.Query("code")
		state := c.Query("state")

		if code == "" || state == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing code or state"})
			return
		}

		sessionID, userInfo, err := manager.HandleCallback(context.Background(), "google", code, state)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Set session cookie
		c.SetSameSite(http.SameSiteLaxMode)
		c.SetCookie(
			sessionCookieName,
			sessionID,
			cookieMaxAge,
			"/",
			"",
			false, // Secure: only HTTPS
			true,  // HttpOnly: not accessible via JavaScript
		)

		c.JSON(http.StatusOK, gin.H{
			"message": fmt.Sprintf("Authenticated as: %s (%s)", userInfo.Name, userInfo.Email),
			"user":    userInfo,
		})
	}
}

// GithubCallbackHandler handles GitHub OAuth2 callback
// @Summary GitHub OAuth2 callback
// @Description Handles GitHub OAuth2 callback and creates session
// @Tags oauth2
// @Produce json
// @Param code query string true "OAuth2 code"
// @Param state query string true "OAuth2 state"
// @Success 200 {object} map[string]string "Authenticated"
// @Failure 401 {object} map[string]string "Unauthorized"
// @Router /auth/callback/github [get]
func GithubCallbackHandler(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		code := c.Query("code")
		state := c.Query("state")

		if code == "" || state == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing code or state"})
			return
		}

		sessionID, userInfo, err := manager.HandleCallback(context.Background(), "github", code, state)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Set session cookie
		c.SetSameSite(http.SameSiteLaxMode)
		c.SetCookie(
			sessionCookieName,
			sessionID,
			cookieMaxAge,
			"/",
			"",
			true, // Secure: only HTTPS
			true, // HttpOnly: not accessible via JavaScript
		)

		c.JSON(http.StatusOK, gin.H{
			"message": fmt.Sprintf("Authenticated as: %s (%s)", userInfo.Name, userInfo.Email),
			"user":    userInfo,
		})
	}
}

// MeHandler returns authenticated user info from session
// @Summary Get authenticated user info
// @Description Returns user info from session
// @Tags oauth2
// @Produce json
// @Success 200 {object} map[string]interface{} "User info"
// @Failure 401 {object} map[string]string "Unauthorized"
// @Router /api/me [get]
func MeHandler(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionID, err := c.Cookie(sessionCookieName)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "no session found"})
			return
		}

		session, err := manager.GetSession(sessionID)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired session"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"user":       session.UserInfo,
			"created_at": session.CreatedAt,
			"expires_at": session.ExpiresAt,
		})
	}
}

// RefreshTokenHandler refreshes the access token using refresh token
// @Summary Refresh access token
// @Description Refreshes access token for OIDC providers (Google)
// @Tags oauth2
// @Produce json
// @Success 200 {object} map[string]string "Token refreshed"
// @Failure 401 {object} map[string]string "Unauthorized"
// @Router /api/refresh [post]
func RefreshTokenHandler(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionID, err := c.Cookie(sessionCookieName)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "no session found"})
			return
		}

		if err := manager.RefreshSession(context.Background(), sessionID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "token refreshed"})
	}
}

// LogoutHandler logs out the user by deleting the session
// @Summary Logout
// @Description Deletes user session and clears cookie
// @Tags oauth2
// @Produce json
// @Success 200 {object} map[string]string "Logged out"
// @Router /auth/logout [post]
func LogoutHandler(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionID, err := c.Cookie(sessionCookieName)
		if err == nil {
			manager.DeleteSession(sessionID)
		}

		// Clear cookie
		c.SetCookie(
			sessionCookieName,
			"",
			-1,
			"/",
			"",
			true,
			true,
		)

		c.JSON(http.StatusOK, gin.H{"message": "logged out"})
	}
}

// AuthMiddleware is a middleware that validates session
func AuthMiddleware(manager *Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionID, err := c.Cookie(sessionCookieName)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "no session found"})
			c.Abort()
			return
		}

		session, err := manager.GetSession(sessionID)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired session"})
			c.Abort()
			return
		}

		// Store session in context for downstream handlers
		c.Set("session", session)
		c.Set("user", session.UserInfo)

		c.Next()
	}
}
