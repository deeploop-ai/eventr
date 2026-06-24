package configspike_test

import (
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	configspike "github.com/eventr/eventr/spike/hocon"
	hoconlib "github.com/gurkankaymak/hocon"
)

func testdataPath(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "testdata", name)
}

func TestLinearHOCON_ParseAndSteps(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")

	conf, err := hoconlib.ParseResource(testdataPath(t, "linear.conf"))
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	tree, err := configspike.ConfigToMap(conf)
	if err != nil {
		t.Fatalf("ConfigToMap: %v", err)
	}

	names, err := configspike.StepNames(tree)
	if err != nil {
		t.Fatalf("StepNames: %v", err)
	}
	want := []string{"kafka-source", "enrich", "filter-high", "kafka-sink"}
	for _, w := range want {
		if !slices.Contains(names, w) {
			t.Fatalf("steps missing %q, got %v", w, names)
		}
	}

	if err := configspike.EnvSubstitutedBrokers(conf, "broker1:9092,broker2:9092"); err != nil {
		t.Fatal(err)
	}

	if err := configspike.OptionalTagAbsent(conf); err != nil {
		t.Fatal(err)
	}

	d, err := configspike.BatchTimeout(tree)
	if err != nil {
		t.Fatalf("BatchTimeout: %v", err)
	}
	if d != time.Second {
		t.Fatalf("batch timeout = %v, want 1s", d)
	}

	if w := conf.GetInt("steps.enrich.transform.workers"); w != 8 {
		t.Fatalf("workers = %d, want 8", w)
	}
}

func TestDAGRoute_DependsOnMap(t *testing.T) {
	conf, err := hoconlib.ParseResource(testdataPath(t, "dag-route.conf"))
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	tree, err := configspike.ConfigToMap(conf)
	if err != nil {
		t.Fatalf("ConfigToMap: %v", err)
	}

	steps := tree["steps"].(map[string]any)
	usSink := steps["us-sink"].(map[string]any)
	route, err := configspike.RouteFromDependsOn(usSink, "splitter")
	if err != nil {
		t.Fatal(err)
	}
	if route != "us" {
		t.Fatalf("route = %q, want us", route)
	}
}

func TestOptionalSubstitution_Present(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("OPTIONAL_TAG", "canary")

	conf, err := hoconlib.ParseResource(testdataPath(t, "linear.conf"))
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}
	tag := conf.GetString("steps.kafka-sink.sink.config.optional_tag")
	if tag != "canary" {
		t.Fatalf("optional_tag = %q, want canary", tag)
	}
}

func TestConfigToMap_RoundTripKeys(t *testing.T) {
	raw := `steps { demo { transform { type = map } } }`
	conf, err := hoconlib.ParseString(raw)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := configspike.ConfigToMap(conf)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tree["steps"].(map[string]any)["demo"]; !ok {
		t.Fatalf("expected steps.demo, got %#v", tree)
	}
}
