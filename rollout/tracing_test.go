package rollout

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	controllerutil "github.com/argoproj/argo-rollouts/utils/controller"
)

func TestRolloutSyncHandlerCreatesIndependentRootSpan(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	rollout := newRollout("checkout", 10, nil, map[string]string{"app": "checkout"})
	rollout.UID = types.UID("rollout-uid")
	rollout.Labels = map[string]string{"private-label": "sensitive-value"}
	rollout.Annotations["private-annotation"] = "sensitive-value"
	rollout.Spec.Strategy.Canary = &v1alpha1.CanaryStrategy{Steps: []v1alpha1.CanaryStep{
		{SetWeight: ptr.To[int32](20)},
		{Analysis: &v1alpha1.RolloutAnalysis{}},
	}}
	rollout.Spec.WorkloadRef = &v1alpha1.ObjectRef{Kind: "Error"}
	rollout.Status.Phase = v1alpha1.RolloutPhaseHealthy
	rollout.Status.ObservedGeneration = "123"
	rollout.Status.CurrentStepIndex = ptr.To[int32](0)
	rollout.Status.Abort = true
	f.rolloutLister = append(f.rolloutLister, rollout)
	f.objects = append(f.objects, rollout)

	controller, _, _ := f.newController(noResyncPeriodFunc)
	tracer, recorder, shutdown := newRolloutTestTracer(t)
	defer shutdown()
	controller.tracer = tracer

	parentCtx, parent := tracer.Start(context.Background(), "parent")
	require.NoError(t, controller.syncHandler(parentCtx, getKey(rollout, t)))
	parent.End()

	span := rolloutSpanByName(t, recorder.Ended(), "controller.Rollout")
	assert.False(t, span.Parent().IsValid(), "reconciliation must not inherit the worker context")
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, "rollout aborted", span.Status().Description)
	assert.Equal(t, map[string]attribute.Value{
		"argo.rollouts.rollout.aborted":             attribute.BoolValue(true),
		"argo.rollouts.rollout.current_step.index":  attribute.Int64Value(0),
		"argo.rollouts.rollout.current_step.type":   attribute.StringValue("set_weight"),
		"argo.rollouts.rollout.current_step.value":  attribute.StringValue("20"),
		"argo.rollouts.rollout.generation":          attribute.Int64Value(123),
		"argo.rollouts.rollout.name":                attribute.StringValue("checkout"),
		"argo.rollouts.rollout.observed_generation": attribute.StringValue("123"),
		"argo.rollouts.rollout.phase":               attribute.StringValue("Degraded"),
		"argo.rollouts.rollout.promote_full":        attribute.BoolValue(false),
		"argo.rollouts.rollout.strategy":            attribute.StringValue("canary"),
		"argo.rollouts.rollout.uid":                 attribute.StringValue("rollout-uid"),
		"k8s.namespace.name":                        attribute.StringValue(metav1.NamespaceDefault),
	}, rolloutSpanAttributes(span))
}

func TestRolloutStepTraceAttributes(t *testing.T) {
	steps := []v1alpha1.CanaryStep{
		{SetWeight: ptr.To[int32](20)},
		{Analysis: &v1alpha1.RolloutAnalysis{}},
	}
	for _, test := range []struct {
		name          string
		index         int32
		expectedType  string
		expectedValue string
	}{
		{name: "set weight", index: 0, expectedType: "set_weight", expectedValue: "20"},
		{name: "analysis", index: 1, expectedType: "analysis"},
		{name: "promoted", index: 2, expectedType: "completed", expectedValue: "promoted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rollout := &v1alpha1.Rollout{
				Spec:   v1alpha1.RolloutSpec{Strategy: v1alpha1.RolloutStrategy{Canary: &v1alpha1.CanaryStrategy{Steps: steps}}},
				Status: v1alpha1.RolloutStatus{CurrentStepIndex: ptr.To(test.index), Phase: v1alpha1.RolloutPhaseHealthy},
			}
			attrs := make(map[string]attribute.Value)
			for _, attr := range appendRolloutStepTraceAttributes(nil, rollout) {
				attrs[string(attr.Key)] = attr.Value
			}
			assert.Equal(t, attribute.Int64Value(int64(test.index)), attrs["argo.rollouts.rollout.current_step.index"])
			assert.Equal(t, attribute.StringValue(test.expectedType), attrs["argo.rollouts.rollout.current_step.type"])
			if test.expectedValue == "" {
				assert.NotContains(t, attrs, "argo.rollouts.rollout.current_step.value")
			} else {
				assert.Equal(t, attribute.StringValue(test.expectedValue), attrs["argo.rollouts.rollout.current_step.value"])
			}
		})
	}
}

func TestCanaryStepTraceValueNormalizesPauseDuration(t *testing.T) {
	validDuration := intstr.FromString("1m")
	stepType, stepValue := canaryStepTraceValue(&v1alpha1.CanaryStep{
		Pause: &v1alpha1.RolloutPause{Duration: &validDuration},
	})
	assert.Equal(t, "pause", stepType)
	assert.Equal(t, "60", stepValue)

	invalidDuration := intstr.FromString("private-duration")
	stepType, stepValue = canaryStepTraceValue(&v1alpha1.CanaryStep{
		Pause: &v1alpha1.RolloutPause{Duration: &invalidDuration},
	})
	assert.Equal(t, "pause", stepType)
	assert.Equal(t, "invalid", stepValue)
}

func TestRolloutTraceStatus(t *testing.T) {
	now := metav1.Now()
	for _, test := range []struct {
		name                string
		status              v1alpha1.RolloutStatus
		expectedCode        codes.Code
		expectedDescription string
	}{
		{name: "healthy", status: v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhaseHealthy}, expectedCode: codes.Unset},
		{name: "progressing", status: v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhaseProgressing}, expectedCode: codes.Unset},
		{name: "degraded", status: v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhaseDegraded}, expectedCode: codes.Error, expectedDescription: "rollout degraded"},
		{name: "abort requested", status: v1alpha1.RolloutStatus{Abort: true}, expectedCode: codes.Error, expectedDescription: "rollout aborted"},
		{name: "abort reconciled", status: v1alpha1.RolloutStatus{AbortedAt: &now}, expectedCode: codes.Error, expectedDescription: "rollout aborted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracer, recorder, shutdown := newRolloutTestTracer(t)
			defer shutdown()
			_, span := tracer.Start(context.Background(), "status")
			setRolloutTraceStatus(span, &v1alpha1.Rollout{Status: test.status})
			span.End()

			status := rolloutSpanByName(t, recorder.Ended(), "status").Status()
			assert.Equal(t, test.expectedCode, status.Code)
			assert.Equal(t, test.expectedDescription, status.Description)
		})
	}
}

func TestRolloutSyncHandlerLeavesStaleCacheRequeueUnset(t *testing.T) {
	f := newFixture(t)
	defer f.Close()

	rollout := newRollout("stale", 1, nil, nil)
	rollout.ResourceVersion = "100"
	rollout.Status.Phase = v1alpha1.RolloutPhaseDegraded
	f.rolloutLister = append(f.rolloutLister, rollout)
	f.objects = append(f.objects, rollout)

	controller, _, _ := f.newController(noResyncPeriodFunc)
	key := getKey(rollout, t)
	controller.rolloutVersionTracker.Record(key, "200")
	tracer, recorder, shutdown := newRolloutTestTracer(t)
	defer shutdown()
	controller.tracer = tracer

	err := controller.syncHandler(context.Background(), key)
	require.ErrorIs(t, err, controllerutil.StaleCacheError)

	span := rolloutSpanByName(t, recorder.Ended(), "controller.Rollout")
	assert.Equal(t, codes.Unset, span.Status().Code)
	assert.Empty(t, span.Events())
}

func TestRolloutSyncHandlerRecordsErrors(t *testing.T) {
	tracer, recorder, shutdown := newRolloutTestTracer(t)
	defer shutdown()
	controller := &Controller{tracer: tracer}

	err := controller.syncHandler(context.Background(), "invalid/key/value")
	require.Error(t, err)

	span := rolloutSpanByName(t, recorder.Ended(), "controller.Rollout")
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, "reconciliation error", span.Status().Description)
	assert.False(t, span.Parent().IsValid())
	assert.Empty(t, span.Events())
}

func TestRolloutSyncHandlerRecordsPanics(t *testing.T) {
	tracer, recorder, shutdown := newRolloutTestTracer(t)
	defer shutdown()
	controller := &Controller{tracer: tracer}

	assert.Panics(t, func() {
		_ = controller.syncHandler(context.Background(), "default/checkout")
	})

	span := rolloutSpanByName(t, recorder.Ended(), "controller.Rollout")
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, "reconciliation panic", span.Status().Description)
	assert.Empty(t, span.Events())
}

func newRolloutTestTracer(t *testing.T) (oteltrace.Tracer, *tracetest.SpanRecorder, func()) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	return provider.Tracer("test"), recorder, func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	}
}

func rolloutSpanByName(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("span %q not found", name)
	return nil
}

func rolloutSpanAttributes(span sdktrace.ReadOnlySpan) map[string]attribute.Value {
	attrs := make(map[string]attribute.Value, len(span.Attributes()))
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value
	}
	return attrs
}
