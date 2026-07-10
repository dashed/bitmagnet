package parity

import (
	"encoding/json"
	"fmt"
)

// Normalizer transforms JSON before differential comparison.
type Normalizer func(json.RawMessage) (json.RawMessage, error)

// CanonicalJSON unmarshals and re-marshals JSON. encoding/json sorts object
// keys during marshaling, producing an order-stable form while preserving
// array order.
func CanonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("unmarshal JSON: %w", err)
	}

	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical JSON: %w", err)
	}
	return canonical, nil
}
