// Package parity provides a language-agnostic differential test harness for
// comparing subsystem implementations against shared fixtures.
//
// Fixtures are newline-delimited JSON objects with this schema:
//
//	{"id":"<string>","subsystem":"<string>","input":<json>,"expected":<json>}
//
// CI runs the package and its subsystem proof tests with:
//
//	go test ./internal/parity/...
package parity
