# Controller tracing

The Argo Rollouts controller can export OpenTelemetry traces over OTLP/gRPC. Tracing is disabled by default and is enabled when `--otlp-address` is set.

For example, add these arguments to the `argo-rollouts` controller container:

```yaml
args:
  - --otlp-address=otel-collector.observability.svc.cluster.local:4317
  - --otlp-insecure=true
  - --otlp-sample-ratio=1
  - --otlp-attrs=deployment.environment.name:production
```

The controller accepts the following tracing options:

| Flag | Default | Description |
| --- | --- | --- |
| `--otlp-address` | empty | OTLP/gRPC collector endpoint. An empty value disables tracing. |
| `--otlp-insecure` | `true` | Use a connection without TLS. Set this to `false` when the collector accepts TLS. |
| `--otlp-headers` | empty | Comma-separated OTLP request headers in `key=value` form. |
| `--otlp-attrs` | empty | Resource attribute in `key:value` form. The flag may be repeated. |
| `--otlp-sample-ratio` | `1` | Fraction of root reconciliation traces to sample, from `0` through `1`. |

The exporter adds `service.name=argo-rollouts` and the controller version as `service.version`. It flushes pending spans during graceful controller shutdown.

## Reconciliation spans

Rollout and AnalysisRun controllers process resources from asynchronous work queues. Each queue attempt therefore starts a separate root span rather than inheriting the controller process context.

| Span | Attributes |
| --- | --- |
| `controller.Rollout` | `k8s.namespace.name`, `argo.rollouts.rollout.name`, `argo.rollouts.rollout.uid`, `argo.rollouts.rollout.generation`, `argo.rollouts.rollout.observed_generation`, `argo.rollouts.rollout.strategy`, `argo.rollouts.rollout.phase`, `argo.rollouts.rollout.current_step.index`, `argo.rollouts.rollout.current_step.type`, `argo.rollouts.rollout.current_step.value`, `argo.rollouts.rollout.promote_full`, `argo.rollouts.rollout.aborted` |
| `controller.AnalysisRun` | `k8s.namespace.name`, `argo.rollouts.analysis_run.name`, `argo.rollouts.analysis_run.uid`, `argo.rollouts.analysis_run.generation`, `argo.rollouts.analysis_run.phase`, `argo.rollouts.analysis_run.metrics.count`, and aggregate `argo.rollouts.analysis_run.metric_results.*.count` values |

The current step type uses a fixed value such as `set_weight`, `analysis`, `pause`, or `completed`. The optional step value contains only a normalized number or controller-defined value, such as `20`, `indefinite`, `invalid`, `all_steps_completed`, or `promoted`. Pause durations are normalized to seconds. The value never contains analysis template names, routing names, plugin names, or an invalid user-supplied duration. The AnalysisRun result counts group metric results by their fixed phase values.

Attributes that do not apply or do not yet have a value are omitted. A resource whose phase is not set records the fixed phase value `Unknown`. A span receives OpenTelemetry error status with the fixed description `reconciliation error` when its handler returns an error. A recovered panic receives the fixed description `reconciliation panic`; the worker's existing panic handling still logs and processes the failure. Normal stale-cache requeues leave span status unset. A Rollout that finishes the attempt degraded or aborted and an AnalysisRun that finishes failed or in error also set error status with a fixed description. Successful and progressing resource states leave span status unset. Error details and resource status messages are not recorded in traces.

The controller does not copy labels, annotations, metric queries, metric arguments, provider configuration, container images, or status messages into spans. Configure extra resource attributes deliberately and avoid sensitive values in command-line flags.
