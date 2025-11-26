package cfg

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type RedisConfig struct {
	Host     string
	Port     string
	Password string
}

type Oauth2Config struct {
	GoogleClientID     string
	GoogleClientSecret string
	GoogleRedirectUrl  string
	GithubClientID     string
	GithubClientSecret string
	GithubRedirectUrl  string
	JWTSecret          string
	JWTExpiration      time.Duration
	StateTimeout       time.Duration
}

type ObservabilityConfig struct {
	OTLPEndpoint string
	ServiceName  string
	Environment  string
}

type PostgresConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

type Config struct {
	AppEnv        string
	NodeId        int
	Redis         RedisConfig
	Postgres      PostgresConfig
	OAuth2        Oauth2Config
	Observability ObservabilityConfig
}

func Load() (*Config, error) {
	var errs []error

	err := godotenv.Load() // ignore if .env missing (local only)
	if err != nil {
		log.Print("skip error godot load env")
	}

	appEnv := mustEnv("APP_ENV", &errs)
	host := mustEnv("REDIS_HOST", &errs)
	port := mustEnv("REDIS_PORT", &errs)
	password := getEnvOrDefault("REDIS_PASSWORD", "")

	nodeIdStr := mustEnv("NODE_ID", &errs)
	nodeId, err := strconv.Atoi(nodeIdStr)
	if err != nil {
		errs = append(errs, fmt.Errorf("NODE_ID must be an integer: %w", err))
	}

	// ==========
	// OAuth2
	// ==========
	googleClientID := mustEnv("GOOGLE_CLIENT_ID", &errs)
	googleClientSecret := mustEnv("GOOGLE_CLIENT_SECRET", &errs)
	googleRedirectUrl := mustEnv("GOOGLE_REDIRECT_URL", &errs)
	githubClientID := mustEnv("GITHUB_CLIENT_ID", &errs)
	githubClientSecret := mustEnv("GITHUB_CLIENT_SECRET", &errs)
	githubRedirectUrl := mustEnv("GITHUB_REDIRECT_URL", &errs)
	jwtSecret := mustEnv("JWT_SECRET", &errs)
	jwtExpirationStr := mustEnv("JWT_EXPIRATION", &errs)
	jwtExpiration, err := time.ParseDuration(jwtExpirationStr)
	if err != nil {
		errs = append(errs, errors.New("invalid duration for JWT_EXPIRATION: "+jwtExpirationStr))
	}
	stateTimeoutStr := mustEnv("STATE_TIMEOUT", &errs)
	stateTimeout, err := time.ParseDuration(stateTimeoutStr)
	if err != nil {
		errs = append(errs, errors.New("invalid duration for STATE_TIMEOUT: "+stateTimeoutStr))
	}

	// ==========
	// Postgres
	// ==========
	pgHost := mustEnv("POSTGRES_HOST", &errs)
	pgPort := mustEnv("POSTGRES_PORT", &errs)
	pgUser := mustEnv("POSTGRES_USER", &errs)
	pgPassword := mustEnv("POSTGRES_PASSWORD", &errs)
	pgDB := mustEnv("POSTGRES_DB", &errs)
	pgSSL := getEnvOrDefault("POSTGRES_SSLMODE", "disable") // default disable

	// ==========
	// Observability
	// ==========
	otlpEndpoint := getEnvOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "alloy.observability.svc.cluster.local:4317")
	serviceName := getEnvOrDefault("OTEL_SERVICE_NAME", "gosdk-app")
	environment := getEnvOrDefault("OTEL_RESOURCE_ATTRIBUTES", appEnv)

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &Config{
		AppEnv: appEnv,
		NodeId: nodeId,
		Redis: RedisConfig{
			Host:     host,
			Port:     port,
			Password: password,
		},
		OAuth2: Oauth2Config{
			GoogleClientID:     googleClientID,
			GoogleClientSecret: googleClientSecret,
			GoogleRedirectUrl:  googleRedirectUrl,
			GithubClientID:     githubClientID,
			GithubClientSecret: githubClientSecret,
			GithubRedirectUrl:  githubRedirectUrl,
			JWTSecret:          jwtSecret,
			JWTExpiration:      jwtExpiration,
			StateTimeout:       stateTimeout,
		},
		Observability: ObservabilityConfig{
			OTLPEndpoint: otlpEndpoint,
			ServiceName:  serviceName,
			Environment:  environment,
		},
		Postgres: PostgresConfig{
			Host:     pgHost,
			Port:     pgPort,
			User:     pgUser,
			Password: pgPassword,
			DBName:   pgDB,
			SSLMode:  pgSSL,
		},
	}, nil
}

// mustEnv appends error into slice instead of returning.
func mustEnv(key string, errs *[]error) string {
	value, exists := os.LookupEnv(key)
	if !exists || value == "" {
		*errs = append(*errs, errors.New("missing env: "+key))
	}
	return value
}

// getEnvOrDefault returns environment variable value or default if not set.
func getEnvOrDefault(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists || value == "" {
		return defaultValue
	}
	return value
}
