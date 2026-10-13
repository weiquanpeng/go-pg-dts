package snapshot

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeDecoder_DecodeJSON(t *testing.T) {
	typeMap := pgtype.NewMap()
	cache := NewDecoderCache()

	t.Run("should keep JSON null as raw text instead of nil", func(t *testing.T) {
		for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
			val, err := cache.Get(oid).Decode(typeMap, []byte("null"))
			require.NoError(t, err)
			assert.Equal(t, json.RawMessage("null"), val)
		}
	})

	t.Run("should preserve JSON string quotes", func(t *testing.T) {
		val, err := cache.Get(pgtype.JSONBOID).Decode(typeMap, []byte(`"hahaha"`))
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"hahaha"`), val)
	})

	t.Run("should preserve object text and big integers", func(t *testing.T) {
		val, err := cache.Get(pgtype.JSONBOID).Decode(typeMap, []byte(`{"n": 9007199254740993}`))
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"n": 9007199254740993}`), val)
	})

	t.Run("should round-trip through json.Marshal verbatim", func(t *testing.T) {
		val, err := cache.Get(pgtype.JSONOID).Decode(typeMap, []byte(`{"a": 1}`))
		require.NoError(t, err)
		marshaled, err := json.Marshal(val)
		require.NoError(t, err)
		assert.Equal(t, `{"a":1}`, string(marshaled))
	})

	t.Run("should still decode non-JSON types via pgtype", func(t *testing.T) {
		val, err := cache.Get(pgtype.Int8OID).Decode(typeMap, []byte("42"))
		require.NoError(t, err)
		assert.Equal(t, int64(42), val)
	})

	t.Run("should use string fallback for unknown OIDs", func(t *testing.T) {
		val, err := cache.Get(999999).Decode(typeMap, []byte("x"))
		require.NoError(t, err)
		assert.Equal(t, "x", val)
	})
}
