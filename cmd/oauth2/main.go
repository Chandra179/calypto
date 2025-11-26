package main

import (
	"context"
	"gosdk/cfg"
	"gosdk/pkg/oauth2"
	"log"

	"github.com/gin-gonic/gin"
)

func main() {
	// ============
	// config
	// ============
	config, errCfg := cfg.Load()
	if errCfg != nil {
		log.Fatal(errCfg)
	}

	// ============
	// Oauth2
	// ============
	oauth2mgr, err := oauth2.NewManager(context.Background(), &config.OAuth2)
	if err != nil {
		log.Fatal(err)
	}

	// ============
	// HTTP
	// ============
	r := gin.Default()
	auth := r.Group("/auth")
	{
		auth.GET("/google", oauth2.GoogleAuthHandler(oauth2mgr))
		auth.GET("/callback/google", oauth2.GoogleCallbackHandler(oauth2mgr))
		auth.GET("/github", oauth2.GithubAuthHandler(oauth2mgr))
		auth.GET("/callback/github", oauth2.GithubCallbackHandler(oauth2mgr))
	}

	protected := r.Group("/auth")
	protected.Use(oauth2.AuthMiddleware(oauth2mgr))
	{
		protected.GET("/me", oauth2.MeHandler(oauth2mgr))
		protected.GET("/refresh", oauth2.RefreshTokenHandler(oauth2mgr))
		protected.GET("/logout", oauth2.LogoutHandler(oauth2mgr))
	}

	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
