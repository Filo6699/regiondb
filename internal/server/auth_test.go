package server

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestAuthFailureTrackerDelaysAndTemporarilyBansSource(t *testing.T) {
	t.Parallel()

	tracker := newAuthFailureTracker(5*time.Millisecond, 2, time.Minute)
	now := time.Unix(100, 0)
	if got := tracker.registerFailure("192.0.2.1", now); got != 5*time.Millisecond {
		t.Fatalf("first failure delay = %v", got)
	}
	if got := tracker.registerFailure("192.0.2.1", now); got != 5*time.Millisecond {
		t.Fatalf("second failure delay = %v", got)
	}
	if !tracker.banned("192.0.2.1", now.Add(10*time.Second)) {
		t.Fatal("source was not temporarily banned")
	}
	if got := tracker.registerFailure("192.0.2.2", now); got != 5*time.Millisecond {
		t.Fatalf("independent source delay = %v", got)
	}

	tracker.authenticated("192.0.2.1")
	if tracker.banned("192.0.2.1", now) {
		t.Fatal("source remained banned after successful authentication")
	}
	if got := tracker.registerFailure("192.0.2.1", now); got != 5*time.Millisecond {
		t.Fatalf("delay after successful authentication = %v", got)
	}
}

func TestAuthFailureTrackerBoundsSourcesWithoutEvictingActiveBans(t *testing.T) {
	t.Parallel()

	tracker := newAuthFailureTracker(time.Millisecond, 1, time.Hour)
	now := time.Unix(100, 0)
	for index := range maxAuthSources {
		tracker.registerFailure(fmt.Sprintf("source-%d", index), now)
	}
	tracker.registerFailure("overflow", now)
	if got := len(tracker.sources); got != maxAuthSources {
		t.Fatalf("tracked sources = %d, want %d", got, maxAuthSources)
	}
	if _, ok := tracker.sources["overflow"]; ok {
		t.Fatal("overflow source displaced an active ban")
	}
	if !tracker.banned("source-0", now.Add(time.Minute)) {
		t.Fatal("oldest active ban was evicted")
	}
}

func TestAuthFailureTrackerEvictsLeastRecentInactiveSource(t *testing.T) {
	t.Parallel()

	tracker := newAuthFailureTracker(time.Millisecond, 2, time.Hour)
	now := time.Unix(100, 0)
	for index := range maxAuthSources {
		tracker.registerFailure(fmt.Sprintf("source-%d", index), now)
	}
	tracker.banned("source-0", now)
	tracker.registerFailure("replacement", now)
	if _, ok := tracker.sources["source-1"]; ok {
		t.Fatal("least recently used inactive source was retained")
	}
	if _, ok := tracker.sources["source-0"]; !ok {
		t.Fatal("recently used inactive source was evicted")
	}
	if _, ok := tracker.sources["replacement"]; !ok {
		t.Fatal("replacement source was not tracked")
	}
}

func TestAuthSourceNormalizesAddressBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		addr net.Addr
		want string
	}{
		{
			name: "IPv4",
			addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 4242},
			want: "192.0.2.10",
		},
		{
			name: "mapped IPv4",
			addr: &net.TCPAddr{IP: net.ParseIP("::ffff:192.0.2.10"), Port: 4242},
			want: "192.0.2.10",
		},
		{
			name: "IPv6 prefix",
			addr: &net.TCPAddr{IP: net.ParseIP("2001:db8:abcd:12::99"), Port: 4242},
			want: "2001:db8:abcd:12::/64",
		},
		{
			name: "IPv6 zone",
			addr: &net.TCPAddr{
				IP:   net.ParseIP("fe80::99"),
				Port: 4242,
				Zone: "eth0",
			},
			want: "fe80::/64",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := remoteAddrConnection{Addr: test.addr}
			if got := authSource(connection); got != test.want {
				t.Fatalf("authSource() = %q, want %q", got, test.want)
			}
		})
	}
}

type remoteAddrConnection struct {
	net.Conn
	net.Addr
}

func (c remoteAddrConnection) RemoteAddr() net.Addr {
	return c.Addr
}
