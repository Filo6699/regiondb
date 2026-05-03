package logging

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLoggerFormatsStructuredLevels(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := NewWithSink(
		newTextSink(&output),
		func() time.Time {
			return time.Date(2026, time.May, 3, 11, 11, 12, 0, time.FixedZone("test", 5*60*60))
		},
	)
	logger.Info("server", "started", slog.String("address", "loopback listener"))
	logger.Warn("server", "draining")
	logger.Error("storage", "close_failed")

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("log line count = %d, want 3: %q", len(lines), output.String())
	}
	pid := strconv.Itoa(logger.pid)
	want := []string{
		`timestamp=2026-05-03T06:11:12.000Z level=info event=started component=server pid=` +
			pid + ` address="loopback listener"`,
		"timestamp=2026-05-03T06:11:12.000Z level=warn event=draining component=server pid=" + pid,
		"timestamp=2026-05-03T06:11:12.000Z level=error event=close_failed component=storage pid=" + pid,
	}
	for index := range want {
		if lines[index] != want[index] {
			t.Errorf("line %d = %q, want %q", index, lines[index], want[index])
		}
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
