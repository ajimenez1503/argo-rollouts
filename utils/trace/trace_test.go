package trace

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInitTracerDisabled(t *testing.T) {
	shutdown, err := InitTracer(context.Background(), "argo-rollouts", "v1.0.0", Config{})
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestInitTracerRejectsInvalidSampleRatio(t *testing.T) {
	for _, ratio := range []float64{-0.01, 1.01, math.NaN()} {
		_, err := InitTracer(context.Background(), "argo-rollouts", "v1.0.0", Config{
			Address:     "collector:4317",
			SampleRatio: ratio,
		})
		require.ErrorContains(t, err, "sample ratio must be between 0 and 1")
	}
}

func TestResourceAttributes(t *testing.T) {
	attrs, err := resourceAttributes([]string{"deployment.environment.name:production", "url:https://collector.example.com:4317"})
	require.NoError(t, err)
	assert.Equal(t, []attribute.KeyValue{
		attribute.String("deployment.environment.name", "production"),
		attribute.String("url", "https://collector.example.com:4317"),
	}, attrs)

	_, err = resourceAttributes([]string{"missing-separator"})
	require.ErrorContains(t, err, "expected key:value")
	_, err = resourceAttributes([]string{":missing-key"})
	require.ErrorContains(t, err, "expected key:value")
}

func TestEndSpanUsesFixedErrorStatus(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	_, span := provider.Tracer("test").Start(context.Background(), "reconcile")

	EndSpan(span, errors.New("private error detail"))

	require.Len(t, recorder.Ended(), 1)
	assert.Equal(t, codes.Error, recorder.Ended()[0].Status().Code)
	assert.Equal(t, "reconciliation error", recorder.Ended()[0].Status().Description)
	assert.Empty(t, recorder.Ended()[0].Events())
}

func TestEndSpanWithPanicUsesFixedErrorStatus(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	_, span := provider.Tracer("test").Start(context.Background(), "reconcile")

	EndSpanWithPanic(span)

	require.Len(t, recorder.Ended(), 1)
	assert.Equal(t, codes.Error, recorder.Ended()[0].Status().Code)
	assert.Equal(t, "reconciliation panic", recorder.Ended()[0].Status().Description)
	assert.Empty(t, recorder.Ended()[0].Events())
}
