package parity

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const maxJSONLLineSize = 64 * 1024 * 1024

// Fixture is one differential test case from a JSONL fixture corpus.
type Fixture struct {
	ID        string          `json:"id"`
	Subsystem string          `json:"subsystem"`
	Input     json.RawMessage `json:"input"`
	Expected  json.RawMessage `json:"expected"`
}

// LoadJSONL reads newline-delimited fixtures from r.
// Blank lines are ignored, while parse errors report their one-based line number.
func LoadJSONL(r io.Reader) ([]Fixture, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxJSONLLineSize)

	var fixtures []Fixture
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var fixture Fixture
		if err := json.Unmarshal(line, &fixture); err != nil {
			return nil, fmt.Errorf("decode JSONL line %d: %w", lineNumber, err)
		}
		fixtures = append(fixtures, fixture)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan JSONL line %d: %w", lineNumber+1, err)
	}

	return fixtures, nil
}

// LoadFile loads newline-delimited fixtures from path.
func LoadFile(path string) ([]Fixture, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open fixture file %q: %w", path, err)
	}
	defer file.Close()

	fixtures, err := LoadJSONL(file)
	if err != nil {
		return nil, fmt.Errorf("load fixture file %q: %w", path, err)
	}
	return fixtures, nil
}
