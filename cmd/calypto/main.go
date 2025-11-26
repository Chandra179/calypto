package main

import (
	"calypto/cfg"
	"calypto/pkg/cache"
	"calypto/pkg/db"
	"calypto/pkg/idgen"
	"calypto/pkg/logger"
	"calypto/pkg/oauth2"
	"calypto/service/wallet"
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "calypto/api" // swagger docs

	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	// logger
	// ============
	zlogger := logger.NewZeroLog(config.AppEnv)

	// ============
	// Otel
	// ============
	shutdownOtel, err := initOtel(context.Background(), &config.Observability, zlogger)
	if err != nil {
		log.Printf("WARNING: failed to initialize OpenTelemetry: %v", err)
		log.Printf("Continuing without tracing/metrics...")
		log.Fatal()
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownOtel(ctx); err != nil {
				log.Printf("failed to shutdown OpenTelemetry: %v", err)
			}
		}()
	}

	// ============
	// Build Postgres DSN from config
	// ============
	pg := config.Postgres
	pgDSN := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		pg.User,
		pg.Password,
		pg.Host,
		pg.Port,
		pg.DBName,
		pg.SSLMode,
	)

	// ============
	// Init DB dbClient
	// ============
	dbClient, err := db.NewSQLClient("postgres", pgDSN)
	if err != nil {
		log.Fatal(err)
	}

	// ============
	// Example transaction
	// ============
	err = dbClient.WithTransaction(context.Background(), sql.LevelSerializable,
		func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO users(id, name) VALUES($1, $2)", 1, "Alice")
			if err != nil {
				return err
			}
			return nil
		})
	if err != nil {
		fmt.Println("Transaction failed:", err)
	} else {
		fmt.Println("Transaction committed successfully")
	}

	// =========
	// Migrate
	// =========
	m, err := migrate.New("file://db/migrations", pgDSN)
	if err != nil {
		log.Fatal(err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatal(err)
	}

	// ============
	// Cache
	// ============
	redisAddr := config.Redis.Host + ":" + config.Redis.Port
	_ = cache.NewRedisCache(redisAddr)

	// ============
	// Oauth2
	// ============
	oauth2mgr, err := oauth2.NewManager(context.Background(), &config.OAuth2)
	if err != nil {
		log.Fatal(err)
	}

	// =============
	// ID Generator
	// =============
	snowFlakeIdGen, err := idgen.NewSnowflakeGenerator(int64(config.NodeId))
	if err != nil {
		log.Fatalf("Failed to initialize ID Generator: %v", err)
	}

	// ============
	// HTTP
	// ============
	r := gin.Default()
	r.Use(otelgin.Middleware(config.Observability.ServiceName))
	r.Use(TraceLoggerMiddleware(zlogger))
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	r.GET("/docs", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		html := `<!DOCTYPE html>
<html>
<head>
    <title>API Documentation</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body>
    <script id="api-reference" data-url="/swagger/doc.json"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</body>
</html>`
		c.String(200, html)
	})

	wallet.NewHandler(r, snowFlakeIdGen, dbClient)

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

// TraceLoggerMiddleware extracts trace_id and span_id from the request context and attaches it to logger
func TraceLoggerMiddleware(log logger.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		span := trace.SpanFromContext(c.Request.Context())
		if span.SpanContext().IsValid() {
			traceID := span.SpanContext().TraceID().String()
			spanID := span.SpanContext().SpanID().String()

			// Store trace info in context for later use
			c.Set("trace_id", traceID)
			c.Set("span_id", spanID)

			log.Info("incoming request",
				logger.Field{Key: "trace_id", Value: traceID},
				logger.Field{Key: "span_id", Value: spanID},
				logger.Field{Key: "method", Value: c.Request.Method},
				logger.Field{Key: "path", Value: c.Request.URL.Path},
			)
		}

		c.Next()

		if span.SpanContext().IsValid() {
			traceID := span.SpanContext().TraceID().String()
			spanID := span.SpanContext().SpanID().String()

			log.Info("request completed",
				logger.Field{Key: "trace_id", Value: traceID},
				logger.Field{Key: "span_id", Value: spanID},
				logger.Field{Key: "status", Value: c.Writer.Status()},
				logger.Field{Key: "method", Value: c.Request.Method},
				logger.Field{Key: "path", Value: c.Request.URL.Path},
			)
		}
	}
}

// initOtel initializes OpenTelemetry tracer and meter with OTLP exporter
func initOtel(ctx context.Context, config *cfg.ObservabilityConfig, log logger.Logger) (func(context.Context) error, error) {
	conn, err := grpc.NewClient(
		config.OTLPEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(config.ServiceName),
			semconv.DeploymentEnvironment(config.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	log.Info("OpenTelemetry initialized - sending to OTLP collector",
		logger.Field{Key: "otlp_endpoint", Value: config.OTLPEndpoint},
	)

	shutdown := func(ctx context.Context) error {
		var errs []error

		if err := tp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("tracer shutdown failed: %w", err))
		}

		if err := mp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("meter shutdown failed: %w", err))
		}

		if len(errs) > 0 {
			return fmt.Errorf("otel shutdown errors: %v", errs)
		}
		return nil
	}

	return shutdown, nil
}
