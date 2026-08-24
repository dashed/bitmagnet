package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const fixtureSHA256 = "0b1ae3f0472e3b4dfdc08a047d242157d896ef7318d5869b500c59137ea68833"

func TestFixtureFreshness(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	generated, err := generate(root)
	if err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(root, fixtureRelativePath)
	committed, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, generated) {
		t.Fatalf("%s is stale; run go run ./tools/parity/dht_runtime_lifecycle -write and review the diff", fixtureRelativePath)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(committed))
	if digest != fixtureSHA256 {
		t.Fatalf("fixture SHA-256 changed: got %s, want %s", digest, fixtureSHA256)
	}
}
