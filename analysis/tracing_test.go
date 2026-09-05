package analysis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

func TestAnalysisRunSyncHandlerCreatesIndependentRootSpan(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	count := intstr.FromInt(1)
	run := &v1alpha1.AnalysisRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "checkout-analysis",
			Namespace:  metav1.NamespaceDefault,
			UID:        types.UID("analysis-run-uid"),
			Generation: 7,
			Labels:     map[string]string{"private-label": "sensitive-value"},
			Annotations: map[string]string{
				"private-annotation":                 "sensitive-value",
				"observability.demo/deployment-id":   "checkout-42",
				"observability.demo/traceparent":     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
				"observability.demo/source-revision": "source-sha",
				"observability.demo/rollback-of":     "checkout-41",
			},
		},
		Spec: v1alpha1.AnalysisRunSpec{
			Metrics: []v1alpha1.Metric{{
				Name:  "private-metric-name",
				Count: &count,
				Provider: v1alpha1.MetricProvider{
					Prometheus: &v1alpha1.PrometheusMetric{},
				},
			}},
		},
	}
	f.analysisRunLister = append(f.analysisRunLister, run)
	f.objects = append(f.objects, run)

	controller, _, _ := f.newController(noResyncPeriodFunc)
	f.provider.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(newMeasurement(v1alpha1.AnalysisPhaseFailed), nil)
	f.provider.On("GetMetadata", mock.Anything, mock.Anything).Return(map[string]string{}, nil)
	tracer, recorder, shutdown := newAnalysisRunTestTracer(t)
	defer shutdown()
	controller.tracer = tracer

	parentCtx, parent := tracer.Start(context.Background(), "parent")
	require.NoError(t, controller.syncHandler(parentCtx, getKey(run, t)))
	parent.End()

	span := analysisRunSpanByName(t, recorder.Ended(), "controller.AnalysisRun")
	assert.False(t, span.Parent().IsValid(), "reconciliation must not inherit the worker context")
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, "analysis run failed", span.Status().Description)
	require.Len(t, span.Links(), 1)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", span.Links()[0].SpanContext.TraceID().String())
	assert.Equal(t, "00f067aa0ba902b7", span.Links()[0].SpanContext.SpanID().String())
	assert.True(t, span.Links()[0].SpanContext.IsRemote())
	assert.Equal(t, map[string]attribute.Value{
		"argo.rollouts.analysis_run.generation":                        attribute.Int64Value(7),
		"argo.rollouts.analysis_run.metric_results.count":              attribute.Int64Value(1),
		"argo.rollouts.analysis_run.metric_results.error.count":        attribute.Int64Value(0),
		"argo.rollouts.analysis_run.metric_results.failed.count":       attribute.Int64Value(1),
		"argo.rollouts.analysis_run.metric_results.inconclusive.count": attribute.Int64Value(0),
		"argo.rollouts.analysis_run.metric_results.pending.count":      attribute.Int64Value(0),
		"argo.rollouts.analysis_run.metric_results.running.count":      attribute.Int64Value(0),
		"argo.rollouts.analysis_run.metric_results.successful.count":   attribute.Int64Value(0),
		"argo.rollouts.analysis_run.metrics.count":                     attribute.Int64Value(1),
		"argo.rollouts.analysis_run.name":                              attribute.StringValue("checkout-analysis"),
		"argo.rollouts.analysis_run.phase":                             attribute.StringValue("Failed"),
		"argo.rollouts.analysis_run.uid":                               attribute.StringValue("analysis-run-uid"),
		"deployment.id":                                                attribute.StringValue("checkout-42"),
		"deployment.rollback_of":                                       attribute.StringValue("checkout-41"),
		"k8s.namespace.name":                                           attribute.StringValue(metav1.NamespaceDefault),
		"vcs.ref.head.revision":                                        attribute.StringValue("source-sha"),
	}, analysisRunSpanAttributes(span))
}

func TestAnalysisRunTraceStatus(t *testing.T) {
	for _, test := range []struct {
		name                string
		phase               v1alpha1.AnalysisPhase
		expectedCode        codes.Code
		expectedDescription string
	}{
		{name: "successful", phase: v1alpha1.AnalysisPhaseSuccessful, expectedCode: codes.Unset},
		{name: "running", phase: v1alpha1.AnalysisPhaseRunning, expectedCode: codes.Unset},
		{name: "failed", phase: v1alpha1.AnalysisPhaseFailed, expectedCode: codes.Error, expectedDescription: "analysis run failed"},
		{name: "error", phase: v1alpha1.AnalysisPhaseError, expectedCode: codes.Error, expectedDescription: "analysis run error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracer, recorder, shutdown := newAnalysisRunTestTracer(t)
			defer shutdown()
			_, span := tracer.Start(context.Background(), "status")
			setAnalysisRunTraceStatus(span, &v1alpha1.AnalysisRun{Status: v1alpha1.AnalysisRunStatus{Phase: test.phase}})
			span.End()

			status := analysisRunSpanByName(t, recorder.Ended(), "status").Status()
			assert.Equal(t, test.expectedCode, status.Code)
			assert.Equal(t, test.expectedDescription, status.Description)
		})
	}
}

func TestAnalysisRunSyncHandlerRecordsErrors(t *testing.T) {
	tracer, recorder, shutdown := newAnalysisRunTestTracer(t)
	defer shutdown()
	controller := &Controller{tracer: tracer}

	err := controller.syncHandler(context.Background(), "invalid/key/value")
	require.Error(t, err)

	span := analysisRunSpanByName(t, recorder.Ended(), "controller.AnalysisRun")
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, "reconciliation error", span.Status().Description)
	assert.False(t, span.Parent().IsValid())
	assert.Empty(t, span.Events())
}

func TestAnalysisRunSyncHandlerRecordsPanics(t *testing.T) {
	tracer, recorder, shutdown := newAnalysisRunTestTracer(t)
	defer shutdown()
	controller := &Controller{tracer: tracer}

	assert.Panics(t, func() {
		_ = controller.syncHandler(context.Background(), "default/checkout-analysis")
	})

	span := analysisRunSpanByName(t, recorder.Ended(), "controller.AnalysisRun")
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, "reconciliation panic", span.Status().Description)
	assert.Empty(t, span.Events())
}

func newAnalysisRunTestTracer(t *testing.T) (oteltrace.Tracer, *tracetest.SpanRecorder, func()) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	return provider.Tracer("test"), recorder, func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	}
}

func analysisRunSpanByName(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("span %q not found", name)
	return nil
}

func analysisRunSpanAttributes(span sdktrace.ReadOnlySpan) map[string]attribute.Value {
	attrs := make(map[string]attribute.Value, len(span.Attributes()))
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value
	}
	return attrs
}
