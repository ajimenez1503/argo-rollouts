package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsPortFlagCompatibility(t *testing.T) {
	cmd := newCommand()

	// Test that both flags are available
	metricsPortFlag := cmd.Flags().Lookup("metricsPort")
	assert.NotNil(t, metricsPortFlag, "metricsPort flag should exist")

	metricsportFlag := cmd.Flags().Lookup("metricsport")
	assert.NotNil(t, metricsportFlag, "metricsport flag should exist for backward compatibility")

	// Test that deprecated flag is marked as deprecated
	assert.True(t, metricsportFlag.Deprecated != "", "metricsport flag should be marked as deprecated")
}

func TestOpenTelemetryFlags(t *testing.T) {
	cmd := newCommand()

	for _, name := range []string{"otlp-address", "otlp-insecure", "otlp-headers", "otlp-attrs", "otlp-sample-ratio"} {
		assert.NotNil(t, cmd.Flags().Lookup(name), "flag %s should exist", name)
	}

	insecure, err := cmd.Flags().GetBool("otlp-insecure")
	require.NoError(t, err)
	assert.True(t, insecure)
	ratio, err := cmd.Flags().GetFloat64("otlp-sample-ratio")
	require.NoError(t, err)
	assert.Equal(t, 1.0, ratio)
}
