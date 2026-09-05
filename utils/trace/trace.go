package trace

import (
	"context"
	"fmt"
	"math"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
)

const (
	// InstrumentationName identifies spans emitted by the Argo Rollouts controller.
	InstrumentationName = "github.com/argoproj/argo-rollouts"
)

// Config controls OpenTelemetry trace export. An empty Address disables tracing.
type Config struct {
	Address     string
	Insecure    bool
	Headers     map[string]string
	Attributes  []string
	SampleRatio float64
}

// ShutdownFunc flushes pending spans and releases exporter resources.
type ShutdownFunc func(context.Context) error

// InitTracer initializes an OTLP/gRPC exporter and installs its provider globally.
// It returns a no-op shutdown function when tracing is disabled.
func InitTracer(ctx context.Context, serviceName, serviceVersion string, cfg Config) (ShutdownFunc, error) {
	if cfg.Address == "" {
		return func(context.Context) error { return nil }, nil
	}
	if math.IsNaN(cfg.SampleRatio) || cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return nil, fmt.Errorf("otlp sample ratio must be between 0 and 1")
	}

	attrs, err := resourceAttributes(cfg.Attributes)
	if err != nil {
		return nil, err
	}
	attrs = append(attrs, attribute.String("service.name", serviceName))
	if serviceVersion != "" {
		attrs = append(attrs, attribute.String("service.version", serviceVersion))
	}

	res, err := resource.New(ctx, resource.WithAttributes(attrs...))
	if err != nil {
		return nil, fmt.Errorf("create OpenTelemetry resource: %w", err)
	}

	exporterOptions := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Address),
		otlptracegrpc.WithHeaders(cfg.Headers),
	}
	if cfg.Insecure {
		exporterOptions = append(exporterOptions, otlptracegrpc.WithInsecure())
	} else {
		exporterOptions = append(exporterOptions, otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, "")))
	}

	exporter, err := otlptracegrpc.New(ctx, exporterOptions...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}

func resourceAttributes(values []string) ([]attribute.KeyValue, error) {
	attrs := make([]attribute.KeyValue, 0, len(values))
	for _, value := range values {
		key, attrValue, found := strings.Cut(value, ":")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			return nil, fmt.Errorf("invalid OTLP resource attribute %q: expected key:value", value)
		}
		attrs = append(attrs, attribute.String(key, strings.TrimSpace(attrValue)))
	}
	return attrs, nil
}

// EndSpan records a fixed error status, when present, and ends span. Error details
// stay in controller logs and are not copied into trace status or events.
func EndSpan(span oteltrace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "reconciliation error")
	}
	span.End()
}

// EndSpanWithPanic records a fixed panic status and ends span. The caller remains
// responsible for re-panicking so the controller worker can perform its normal recovery.
func EndSpanWithPanic(span oteltrace.Span) {
	span.SetStatus(codes.Error, "reconciliation panic")
	span.End()
}
