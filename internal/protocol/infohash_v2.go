package protocol

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// InfoHashV2Length is the length in bytes of a BitTorrent v2 (BEP 52) info hash,
// which is the full SHA-256 of the info dictionary.
const InfoHashV2Length = 32

// InfoHashV2 is a BitTorrent v2 (BEP 52) info hash: the full 32-byte SHA-256 of
// the info dictionary.
//
// Unlike [ID] (the 20-byte SHA-1 v1 info hash / DHT node ID), InfoHashV2 is NOT
// used as a primary key, map key, or on the DHT/peer wire — where 20 bytes are
// required BEP 52 mandates the truncated (first 20 bytes) form, which is stored
// as the canonical [ID] primary key. InfoHashV2 records the full v2 identity in
// the dedicated info_hash_v2 column so it can be exposed and looked up later.
type InfoHashV2 [InfoHashV2Length]byte

// ParseInfoHashV2 parses a 64-character hex string (optionally 0x-prefixed) into
// an InfoHashV2.
func ParseInfoHashV2(str string) (InfoHashV2, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(str, "0x"))
	if err != nil {
		return InfoHashV2{}, err
	}

	if len(b) != InfoHashV2Length {
		return InfoHashV2{}, errors.New("hash string must be 32 bytes")
	}

	var h InfoHashV2

	copy(h[:], b)

	return h, nil
}

// NewInfoHashV2FromByteSlice builds an InfoHashV2 from a 32-byte slice.
func NewInfoHashV2FromByteSlice(b []byte) (h InfoHashV2, _ error) {
	if len(b) != InfoHashV2Length {
		return h, errors.New("must be 32 bytes")
	}

	copy(h[:], b)

	return h, nil
}

func (h InfoHashV2) String() string {
	return hex.EncodeToString(h[:])
}

func (h InfoHashV2) Bytes() []byte {
	return h[:]
}

func (h InfoHashV2) IsZero() bool {
	return h == InfoHashV2{}
}

// ToShort returns the truncated (first 20 bytes) form of the v2 hash, which is
// the value used wherever a 20-byte hash is required (DHT, the canonical primary
// key for pure-v2 torrents) per BEP 52.
func (h InfoHashV2) ToShort() (id ID) {
	copy(id[:], h[:len(id)])
	return
}

func (h *InfoHashV2) Scan(value interface{}) error {
	v, ok := value.([]byte)
	if !ok {
		return errors.New("invalid bytes type")
	}

	if len(v) != InfoHashV2Length {
		return errors.New("invalid InfoHashV2 length")
	}

	copy(h[:], v)

	return nil
}

func (h InfoHashV2) Value() (driver.Value, error) {
	return h[:], nil
}

func (h InfoHashV2) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.String())
}

func (h *InfoHashV2) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsed, err := ParseInfoHashV2(s)
	if err != nil {
		return err
	}

	*h = parsed

	return nil
}

// UnmarshalGQL implements the gqlgen Unmarshaler interface for the Hash32 scalar.
func (h *InfoHashV2) UnmarshalGQL(input interface{}) error {
	switch input := input.(type) {
	case string:
		parsed, err := ParseInfoHashV2(input)
		if err != nil {
			return err
		}

		*h = parsed

		return nil
	default:
		return errors.New("invalid hash type")
	}
}

// MarshalGQL implements the gqlgen Marshaler interface for the Hash32 scalar. It
// uses a value receiver so that both InfoHashV2 and *InfoHashV2 satisfy
// graphql.Marshaler (the generated nullable-pointer marshaller returns the value
// directly, with nil rendered as null).
func (h InfoHashV2) MarshalGQL(w io.Writer) {
	_, _ = w.Write([]byte(`"` + h.String() + `"`))
}
