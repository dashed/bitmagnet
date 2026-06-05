package protocol

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleV2 is a deterministic non-zero 32-byte v2 hash used across the tests.
func sampleV2() InfoHashV2 {
	var h InfoHashV2
	for i := range h {
		h[i] = byte(i + 1)
	}

	return h
}

func TestParseInfoHashV2RoundTrip(t *testing.T) {
	t.Parallel()

	h := sampleV2()
	str := h.String()

	assert.Len(t, str, 64, "v2 hash must render as 64 hex chars")

	parsed, err := ParseInfoHashV2(str)
	require.NoError(t, err)
	assert.Equal(t, h, parsed)
}

func TestParseInfoHashV2Variants(t *testing.T) {
	t.Parallel()

	h := sampleV2()
	lower := h.String()

	tests := []struct {
		name    string
		input   string
		wantErr bool
		want    InfoHashV2
	}{
		{name: "plain lowercase", input: lower, want: h},
		{name: "0x prefixed", input: "0x" + lower, want: h},
		{name: "uppercase", input: "0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20", want: h},
		{name: "too short", input: "0102", wantErr: true},
		{name: "not hex", input: "zz" + lower[2:], wantErr: true},
		{name: "20 bytes (v1 length)", input: "0102030405060708090a0b0c0d0e0f1011121314", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseInfoHashV2(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewInfoHashV2FromByteSlice(t *testing.T) {
	t.Parallel()

	h := sampleV2()

	got, err := NewInfoHashV2FromByteSlice(h.Bytes())
	require.NoError(t, err)
	assert.Equal(t, h, got)

	_, err = NewInfoHashV2FromByteSlice(make([]byte, 20))
	require.Error(t, err, "20-byte slice must be rejected")

	_, err = NewInfoHashV2FromByteSlice(make([]byte, 33))
	assert.Error(t, err, "33-byte slice must be rejected")
}

func TestInfoHashV2ScanValue(t *testing.T) {
	t.Parallel()

	h := sampleV2()

	v, err := h.Value()
	require.NoError(t, err)

	b, ok := v.([]byte)
	require.True(t, ok, "Value must return []byte")
	assert.Len(t, b, InfoHashV2Length)

	var scanned InfoHashV2

	require.NoError(t, scanned.Scan(b))
	assert.Equal(t, h, scanned)
}

func TestInfoHashV2ScanRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value interface{}
	}{
		{name: "20-byte (v1 length)", value: make([]byte, 20)},
		{name: "wrong length 31", value: make([]byte, 31)},
		{name: "wrong length 33", value: make([]byte, 33)},
		{name: "non-bytes type", value: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"},
		{name: "nil", value: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h InfoHashV2

			assert.Error(t, h.Scan(tt.value))
		})
	}
}

func TestInfoHashV2ToShort(t *testing.T) {
	t.Parallel()

	h := sampleV2()
	short := h.ToShort()

	assert.Equal(t, h[:20], short[:], "ToShort must return the first 20 bytes")

	var wantFirst20 ID

	copy(wantFirst20[:], h[:20])
	assert.Equal(t, wantFirst20, short)
}

func TestInfoHashV2JSON(t *testing.T) {
	t.Parallel()

	h := sampleV2()

	data, err := json.Marshal(h)
	require.NoError(t, err)
	assert.JSONEq(t, `"`+h.String()+`"`, string(data))

	var decoded InfoHashV2

	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, h, decoded)

	assert.Error(t, json.Unmarshal([]byte(`"not-hex"`), &decoded))
}

func TestInfoHashV2IsZero(t *testing.T) {
	t.Parallel()

	var zero InfoHashV2

	assert.True(t, zero.IsZero())

	assert.False(t, sampleV2().IsZero())
}

func TestInfoHashV2MarshalGQL(t *testing.T) {
	t.Parallel()

	h := sampleV2()

	var buf bytes.Buffer

	h.MarshalGQL(&buf)

	// gqlgen scalars marshal as a JSON string literal (quoted 64-hex).
	assert.Equal(t, `"`+h.String()+`"`, buf.String())
	assert.Len(t, h.String(), 64)
}

func TestInfoHashV2UnmarshalGQL(t *testing.T) {
	t.Parallel()

	h := sampleV2()

	t.Run("round-trip from hex string", func(t *testing.T) {
		t.Parallel()

		var got InfoHashV2

		require.NoError(t, got.UnmarshalGQL(h.String()))
		assert.Equal(t, h, got)
	})

	t.Run("accepts 0x prefix", func(t *testing.T) {
		t.Parallel()

		var got InfoHashV2

		require.NoError(t, got.UnmarshalGQL("0x"+h.String()))
		assert.Equal(t, h, got)
	})

	t.Run("rejects non-string input", func(t *testing.T) {
		t.Parallel()

		var got InfoHashV2

		assert.Error(t, got.UnmarshalGQL(1234))
	})

	t.Run("rejects malformed hex", func(t *testing.T) {
		t.Parallel()

		var got InfoHashV2

		assert.Error(t, got.UnmarshalGQL("not-a-valid-hash"))
	})
}
