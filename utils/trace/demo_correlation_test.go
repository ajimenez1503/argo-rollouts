package trace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const validDemoTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestApplyDemoCorrelationUsesFixedAllowlistAndSpanLink(t *testing.T) {
	span, recorder, shutdown := newDemoCorrelationTestSpan(t)
	defer shutdown()

	ApplyDemoCorrelation(span, map[string]string{
		demoDeploymentIDAnnotation:   "checkout-42",
		demoTraceparentAnnotation:    validDemoTraceparent,
		demoSourceRevisionAnnotation: "source-sha",
		demoRollbackOfAnnotation:     "checkout-41",
		"example.com/secret":         "do-not-export",
	})
	span.End()

	recorded := recorder.Ended()[0]
	assert.Equal(t, map[string]attribute.Value{
		"deployment.id":          attribute.StringValue("checkout-42"),
		"deployment.rollback_of": attribute.StringValue("checkout-41"),
		"vcs.ref.head.revision":  attribute.StringValue("source-sha"),
	}, recordedAttributes(recorded))
	require.Len(t, recorded.Links(), 1)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", recorded.Links()[0].SpanContext.TraceID().String())
	assert.Equal(t, "00f067aa0ba902b7", recorded.Links()[0].SpanContext.SpanID().String())
	assert.True(t, recorded.Links()[0].SpanContext.IsRemote())
}

func TestApplyDemoCorrelationIgnoresInvalidAndEmptyValues(t *testing.T) {
	span, recorder, shutdown := newDemoCorrelationTestSpan(t)
	defer shutdown()

	ApplyDemoCorrelation(span, map[string]string{
		demoDeploymentIDAnnotation:   " ",
		demoTraceparentAnnotation:    "not-a-traceparent",
		demoSourceRevisionAnnotation: "",
		demoRollbackOfAnnotation:     "\t",
	})
	span.End()

	recorded := recorder.Ended()[0]
	assert.Empty(t, recorded.Attributes())
	assert.Empty(t, recorded.Links())
}

func TestApplyDemoCorrelationHandlesNilAnnotations(t *testing.T) {
	span, recorder, shutdown := newDemoCorrelationTestSpan(t)
	defer shutdown()

	ApplyDemoCorrelation(span, nil)
	span.End()

	recorded := recorder.Ended()[0]
	assert.Empty(t, recorded.Attributes())
	assert.Empty(t, recorded.Links())
}

func newDemoCorrelationTestSpan(t *testing.T) (sdktrace.ReadWriteSpan, *tracetest.SpanRecorder, func()) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	_, span := provider.Tracer("test").Start(context.Background(), "controller.Reconciliation")
	readWriteSpan, ok := span.(sdktrace.ReadWriteSpan)
	require.True(t, ok)
	return readWriteSpan, recorder, func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	}
}

func recordedAttributes(span sdktrace.ReadOnlySpan) map[string]attribute.Value {
	attrs := make(map[string]attribute.Value, len(span.Attributes()))
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value
	}
	return attrs
}
