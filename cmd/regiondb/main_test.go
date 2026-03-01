package main

import (
	"bytes"
	"testing"
)

func TestPrintVersion(t *testing.T) {
	var output bytes.Buffer

	if err := printVersion(&output); err != nil {
		t.Fatalf("printVersion() error = %v", err)
	}

	const want = "regiondb dev\n"
	if got := output.String(); got != want {
		t.Fatalf("printVersion() = %q, want %q", got, want)
	}
}
