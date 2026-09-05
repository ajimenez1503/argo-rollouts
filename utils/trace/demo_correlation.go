package trace

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	demoDeploymentIDAnnotation   = "observability.demo/deployment-id"
	demoTraceparentAnnotation    = "observability.demo/traceparent"
	demoSourceRevisionAnnotation = "observability.demo/source-revision"
	demoRollbackOfAnnotation     = "observability.demo/rollback-of"
)

// ApplyDemoCorrelation copies only the demo's fixed correlation annotation
// allowlist onto a reconciliation span. The CI context is represented as a
// span link because controller queue processing is an asynchronous boundary.
func ApplyDemoCorrelation(span oteltrace.Span, annotations map[string]string) {
	if len(annotations) == 0 {
		return
	}

	attrs := make([]attribute.KeyValue, 0, 3)
	if value := strings.TrimSpace(annotations[demoDeploymentIDAnnotation]); value != "" {
		attrs = append(attrs, attribute.String("deployment.id", value))
	}
	if value := strings.TrimSpace(annotations[demoSourceRevisionAnnotation]); value != "" {
		attrs = append(attrs, attribute.String("vcs.ref.head.revision", value))
	}
	if value := strings.TrimSpace(annotations[demoRollbackOfAnnotation]); value != "" {
		attrs = append(attrs, attribute.String("deployment.rollback_of", value))
	}
	span.SetAttributes(attrs...)

	traceparent := strings.TrimSpace(annotations[demoTraceparentAnnotation])
	if traceparent == "" {
		return
	}
	extracted := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{
		"traceparent": traceparent,
	})
	spanContext := oteltrace.SpanContextFromContext(extracted)
	if spanContext.IsValid() {
		span.AddLink(oteltrace.Link{SpanContext: spanContext})
	}
}
