package logging

import (
	"bytes"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestLoggerFormatsStructuredLevels(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := New(&output)
	logger.Info("server", "started", slog.String("address", "loopback listener"))
	logger.Warn("server", "draining")
	logger.Error("storage", "close_failed")

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("log line count = %d, want 3: %q", len(lines), output.String())
	}
	pid := strconv.Itoa(logger.pid)
	for index, level := range []string{"info", "warn", "error"} {
		pattern := `^timestamp=\S+Z level=` + level +
			` event=\S+ component=\S+ pid=` + pid
		if matched := regexp.MustCompile(pattern).MatchString(lines[index]); !matched {
			t.Errorf("line %d = %q, want pattern %q", index, lines[index], pattern)
		}
	}
	if !strings.Contains(lines[0], `address="loopback listener"`) {
		t.Fatalf("escaped field missing from %q", lines[0])
	}
}

func TestLoggerOmitsSecretAndPathFields(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := New(&output)
	logger.Error("storage", "open_failed",
		slog.String("token", "auth-secret"),
		slog.String("data_dir", "/private/world"),
		slog.String("certificate_path", "/private/server.crt"),
		slog.String("mode", "writer"),
	)

	got := output.String()
	for _, secret := range []string{"auth-secret", "/private/world", "/private/server.crt"} {
		if strings.Contains(got, secret) {
			t.Errorf("log contains secret %q: %q", secret, got)
		}
	}
	if !strings.Contains(got, "mode=writer") {
		t.Fatalf("safe field missing from %q", got)
	}
}
