package obstest

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
)

func TestHarnessCapturesSignalsWithoutProviderReset(t *testing.T) {
	h := New(t)
	ctx, span := h.TracerProvider().Tracer("test").Start(context.Background(), "parent")
	h.Logger("test").Warn(ctx, "warning", "component", "harness")
	counter, err := h.MeterProvider().Meter("test").Int64Counter("test.counter")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "ok")))
	span.End()
	if len(h.SpansByName("parent")) != 1 || len(h.LogsBySeverity(log.SeverityWarn)) != 1 {
		t.Fatal("harness did not capture trace and log")
	}
	if got, ok := h.CounterValue(context.Background(), "test.counter", attribute.String("result", "ok")); !ok || got != 1 {
		t.Fatalf("counter = %d, %v", got, ok)
	}
}

func TestProductionDependenciesDoNotIncludeForbiddenSDKs(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "list", "-deps", "./database")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	importsCmd := exec.Command("go", "list", "-f", "{{join .Imports \"\\n\"}}", "./database")
	importsCmd.Dir = root
	imports, _ := importsCmd.Output()
	legacyTelemetry := strings.Contains(string(imports), "github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry")
	for _, forbidden := range []string{"go.opentelemetry.io/otel/sdk", "go.uber.org/zap", "exporters/otlp", "telemetrytest"} {
		// The deprecated telemetry.Client adapter owns the SDK until Phase C
		// removes the legacy import. The guard becomes strict automatically then.
		if legacyTelemetry && forbidden != "telemetrytest" {
			continue
		}
		for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if dep == forbidden || strings.Contains(dep, forbidden) {
				t.Fatalf("production dependency contains %q: %s", forbidden, dep)
			}
		}
	}
}
