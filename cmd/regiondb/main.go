package main

import (
	"io"
	"os"
)

const version = "dev"

func main() {
	if err := printVersion(os.Stdout); err != nil {
		os.Exit(1)
	}
}

func printVersion(w io.Writer) error {
	_, err := io.WriteString(w, "regiondb "+version+"\n")
	return err
}
