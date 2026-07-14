package telemetry

import (
	"strings"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/storage"
)

func TestMetricsPrometheusSnapshotHasBoundedSeries(t *testing.T) {
	t.Parallel()

	var metrics Metrics
	metrics.ObserveCommand(750*time.Microsecond, false)
	metrics.ObserveCommand(2*time.Second, true)
	metrics.AuthFailure()
	metrics.AuthBan()
	metrics.ConnectionOpened()
	metrics.SetAuthSources(12)

	got := string(metrics.AppendPrometheus(nil, storage.RuntimeStats{
		CacheHits:      3,
		LoadedChunks:   4,
		OpenWALHandles: 5,
	}))
	for _, want := range []string{
		"regiondb_commands_total 2\n",
		"regiondb_command_errors_total 1\n",
		"regiondb_command_duration_seconds_bucket{le=\"0.001\"} 1\n",
		"regiondb_command_duration_seconds_bucket{le=\"5\"} 2\n",
		"regiondb_command_duration_seconds_bucket{le=\"+Inf\"} 2\n",
		"regiondb_command_duration_seconds_count 2\n",
		"regiondb_auth_failures_total 1\n",
		"regiondb_auth_bans_total 1\n",
		"regiondb_connections 1\n",
		"regiondb_auth_sources 12\n",
		"regiondb_cache_hits_total 3\n",
		"regiondb_loaded_chunks 4\n",
		"regiondb_open_wal_handles 5\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("snapshot does not contain %q:\n%s", want, got)
		}
	}
	if gotCount := strings.Count(got, "\n"); gotCount != 32 {
		t.Fatalf("metric series = %d, want fixed 32", gotCount)
	}
}

func TestMetricsConnectionGaugeReturnsToZero(t *testing.T) {
	t.Parallel()

	var metrics Metrics
	metrics.ConnectionOpened()
	metrics.ConnectionClosed()
	got := string(metrics.AppendPrometheus(nil, storage.RuntimeStats{}))
	if !strings.Contains(got, "regiondb_connections 0\n") {
		t.Fatalf("snapshot has unexpected connection gauge:\n%s", got)
	}
}
