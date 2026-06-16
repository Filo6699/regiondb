package server

import "testing"

func TestParseURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    Endpoint
		wantErr bool
	}{
		{
			name: "DNS host",
			raw:  "region://secret@example.com:8123/",
			want: Endpoint{Token: "secret", Address: "example.com:8123"},
		},
		{
			name: "IPv6 host",
			raw:  "region://secret@[::1]:8123/",
			want: Endpoint{Token: "secret", Address: "[::1]:8123"},
		},
		{
			name: "escaped token",
			raw:  "region://a%3Ab@localhost:1/",
			want: Endpoint{Token: "a:b", Address: "localhost:1"},
		},
		{
			name: "TLS",
			raw:  "regions://secret@localhost:8123/",
			want: Endpoint{Token: "secret", Address: "localhost:8123", TLS: true},
		},
		{name: "empty", raw: "", wantErr: true},
		{name: "unknown scheme", raw: "http://secret@localhost:1/", wantErr: true},
		{name: "missing token", raw: "region://localhost:1/", wantErr: true},
		{name: "password syntax", raw: "region://secret:password@localhost:1/", wantErr: true},
		{name: "missing host", raw: "region://secret@:1/", wantErr: true},
		{name: "missing port", raw: "region://secret@localhost/", wantErr: true},
		{name: "zero port", raw: "region://secret@localhost:0/", wantErr: true},
		{name: "port overflow", raw: "region://secret@localhost:65536/", wantErr: true},
		{name: "missing root slash", raw: "region://secret@localhost:1", wantErr: true},
		{name: "extra path", raw: "region://secret@localhost:1/world", wantErr: true},
		{name: "query", raw: "region://secret@localhost:1/?x=1", wantErr: true},
		{name: "fragment", raw: "region://secret@localhost:1/#x", wantErr: true},
		{name: "space in token", raw: "region://bad%20token@localhost:1/", wantErr: true},
		{name: "unbracketed IPv6", raw: "region://secret@::1:8123/", wantErr: true},
		{name: "unclosed IPv6 bracket", raw: "region://secret@[::1:8123/", wantErr: true},
		{name: "IPv6 missing port", raw: "region://secret@[::1]/", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseURI(test.raw)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ParseURI(%q) succeeded: %+v", test.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseURI(%q) error = %v", test.raw, err)
			}
			if got != test.want {
				t.Fatalf("ParseURI(%q) = %+v, want %+v", test.raw, got, test.want)
			}
		})
	}
}
