package protocol

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseFrame(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		frame   string
		want    Command
		wantErr bool
	}{
		{
			name:  "command without arguments",
			frame: "PING\r\n",
			want:  Command{Name: "PING", Args: []string{}},
		},
		{
			name:  "command with arguments",
			frame: "SET -12 34 7\r\n",
			want:  Command{Name: "SET", Args: []string{"-12", "34", "7"}},
		},
		{name: "empty", frame: "\r\n", wantErr: true},
		{name: "missing CRLF", frame: "PING\n", wantErr: true},
		{name: "embedded line", frame: "PING\r\nQUIT\r\n", wantErr: true},
		{name: "leading space", frame: " PING\r\n", wantErr: true},
		{name: "double space", frame: "GET  1 2\r\n", wantErr: true},
		{name: "tab separator", frame: "GET\t1\t2\r\n", wantErr: true},
		{name: "lowercase name", frame: "ping\r\n", wantErr: true},
		{name: "non ASCII", frame: "AUTH секрет\r\n", wantErr: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseFrame([]byte(test.frame))
			if test.wantErr {
				if !errors.Is(err, ErrInvalidFrame) {
					t.Fatalf("ParseFrame() error = %v, want ErrInvalidFrame", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFrame() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ParseFrame() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestSerializeCommand(t *testing.T) {
	t.Parallel()

	got, err := SerializeCommand("SET", "-12", "34", "7")
	if err != nil {
		t.Fatalf("SerializeCommand() error = %v", err)
	}
	if want := "SET -12 34 7\r\n"; string(got) != want {
		t.Fatalf("SerializeCommand() = %q, want %q", got, want)
	}
	for _, argument := range []string{"safe\rQUIT", "safe\nQUIT", "safe\r\nQUIT"} {
		if _, err := SerializeCommand("AUTH", argument); !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("SerializeCommand(%q) error = %v, want ErrInvalidFrame", argument, err)
		}
	}
}

func BenchmarkParseFrameReuse(b *testing.B) {
	frame := []byte("SET -12 34 7\r\n")
	scratch := make([]string, 0, 4)

	b.ReportAllocs()
	for range b.N {
		command, err := parseFrame(frame, scratch[:0])
		if err != nil {
			b.Fatal(err)
		}
		scratch = command.Args
	}
}

func TestResponseFraming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response Response
		want     string
	}{
		{name: "plain OK", response: okResponse(""), want: "+OK\r\n"},
		{name: "detailed OK", response: okResponse("PONG"), want: "+OK PONG\r\n"},
		{
			name:     "error",
			response: errorResponse("ARITY", "wrong number of arguments"),
			want:     "-ERR ARITY wrong number of arguments\r\n",
		},
		{name: "bulk", response: bulkResponse([]byte("123")), want: "$3\r\n123\r\n"},
		{name: "empty bulk", response: bulkResponse(nil), want: "$0\r\n\r\n"},
		{
			name:     "array",
			response: arrayResponse([][]byte{[]byte("1"), []byte("23"), nil}),
			want:     "*3\r\n$1\r\n1\r\n$2\r\n23\r\n$0\r\n\r\n",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := string(test.response.Bytes()); got != test.want {
				t.Fatalf("Response.Bytes() = %q, want %q", got, test.want)
			}
		})
	}
}
