package main

import (
	"context"
	"fmt"
	"gosdk/cfg"
	"log"
	"time"

	"github.com/gin-gonic/gin"
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
	// Otel
	// ============
	shutdownOtel, err := initOtel(context.Background(), &config.Observability)
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
}

// TraceLoggerMiddleware extracts trace_id and span_id from the request context and attaches it to logger
func TraceLoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		span := trace.SpanFromContext(c.Request.Context())
		if span.SpanContext().IsValid() {
			traceID := span.SpanContext().TraceID().String()
			spanID := span.SpanContext().SpanID().String()

			// Store trace info in context for later use
			c.Set("trace_id", traceID)
			c.Set("span_id", spanID)

			log.Print("incoming request")
		}

		c.Next()

		if span.SpanContext().IsValid() {
			traceID := span.SpanContext().TraceID().String()
			spanID := span.SpanContext().SpanID().String()

			log.Print("request completed, trace_id: " + traceID + " span_id: " + spanID)
		}
	}
}

// initOtel initializes OpenTelemetry tracer and meter with OTLP exporter
func initOtel(ctx context.Context, config *cfg.ObservabilityConfig) (func(context.Context) error, error) {
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

	log.Print("OpenTelemetry initialized - sending to OTLP collector: " + config.OTLPEndpoint)

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
