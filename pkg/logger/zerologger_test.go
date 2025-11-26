package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestZeroLogger_Info(t *testing.T) {
	buf := &bytes.Buffer{}
	log := NewWithWriter("development", buf)

	log.Info("info-test", Field{Key: "key", Value: "value"})

	output := buf.String()

	if !strings.Contains(output, "info-test") {
		t.Errorf("expected 'info-test' in log, got: %s", output)
	}
	if !strings.Contains(output, `"key":"value"`) {
		t.Errorf("expected field key=value, got: %s", output)
	}
	if !strings.Contains(output, `"level":"info"`) {
		t.Errorf("expected level=info, got: %s", output)
	}
}

func TestZeroLogger_DebugShownInDev(t *testing.T) {
	buf := &bytes.Buffer{}
	log := NewWithWriter("development", buf)

	log.Debug("debug-test")

	output := buf.String()
	if !strings.Contains(output, "debug-test") {
		t.Errorf("expected debug log in development, got: %s", output)
	}
}

func TestZeroLogger_DebugHiddenInProduction(t *testing.T) {
	buf := &bytes.Buffer{}
	log := NewWithWriter("production", buf)

	log.Debug("debug-hidden") // should NOT appear

	output := buf.String()
	if output != "" {
		t.Errorf("expected NO debug log output in production, got: %s", output)
	}
}

func TestZeroLogger_Warn(t *testing.T) {
	buf := &bytes.Buffer{}
	log := NewWithWriter("development", buf)

	log.Warn("warn-test", Field{Key: "warn", Value: "yes"})

	output := buf.String()

	if !strings.Contains(output, `"level":"warn"`) {
		t.Errorf("expected warn level, got: %s", output)
	}
	if !strings.Contains(output, `"warn":"yes"`) {
		t.Errorf("expected field warn=yes, got: %s", output)
	}
}

func TestZeroLogger_Error(t *testing.T) {
	buf := &bytes.Buffer{}
	log := NewWithWriter("development", buf)

	log.Error("error-test")

	output := buf.String()

	if !strings.Contains(output, `"level":"error"`) {
		t.Errorf("expected error level, got: %s", output)
	}
}
