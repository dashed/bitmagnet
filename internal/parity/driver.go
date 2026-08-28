package parity

import "encoding/json"

// Driver runs one subsystem implementation against fixture input.
type Driver interface {
	Subsystem() string
	Run(input json.RawMessage) (json.RawMessage, error)
}
