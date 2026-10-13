package tuple

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewData(t *testing.T) {
	tupleDataType := uint8('D')

	// Payload construction
	buf := []byte{}
	// Skip bytes (simulated garbage or other headers)
	skipBytes := []byte{0x00}
	buf = append(buf, skipBytes...)

	// Tuple Data Type
	buf = append(buf, tupleDataType)

	// Column count (2)
	colCount := make([]byte, 2)
	binary.BigEndian.PutUint16(colCount, 2)
	buf = append(buf, colCount...)

	// Col 1: Text 't'
	buf = append(buf, DataTypeText)
	// Col 1 Length: 5 ("value")
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 5)
	buf = append(buf, lenBuf...)
	// Col 1 Data
	buf = append(buf, []byte("value")...)

	// Col 2: Null 'n'
	buf = append(buf, DataTypeNull)

	t.Run("should parse valid data", func(t *testing.T) {
		d, err := NewData(buf, tupleDataType, len(skipBytes))
		require.NoError(t, err)
		assert.NotNil(t, d)
		assert.Equal(t, uint16(2), d.ColumnNumber)
		assert.Len(t, d.Columns, 2)

		assert.Equal(t, DataTypeText, d.Columns[0].DataType)
		assert.Equal(t, uint32(5), d.Columns[0].Length)
		assert.Equal(t, []byte("value"), d.Columns[0].Data)

		assert.Equal(t, DataTypeNull, d.Columns[1].DataType)
	})

	t.Run("should return error for invalid tuple data type", func(t *testing.T) {
		_, err := NewData(buf, 'X', len(skipBytes)) // Expecting 'X', but got 'D'
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid tuple data type: D")
	})
}

func TestData_DecodeWithColumn(t *testing.T) {
	tupleDataType := uint8('D')
	buf := []byte{}
	buf = append(buf, tupleDataType) // No skip bytes this time for simplicity

	// Column count (2)
	colCount := make([]byte, 2)
	binary.BigEndian.PutUint16(colCount, 2)
	buf = append(buf, colCount...)

	// Col 1: Text 't' - Int value "123"
	buf = append(buf, DataTypeText)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 3)
	buf = append(buf, lenBuf...)
	buf = append(buf, []byte("123")...)

	// Col 2: Null 'n'
	buf = append(buf, DataTypeNull)

	d, _ := NewData(buf, tupleDataType, 0)

	t.Run("should decode columns correctly", func(t *testing.T) {
		relationColumns := []RelationColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "description", DataType: pgtype.TextOID},
		}

		decoded, err := d.DecodeWithColumn(relationColumns)
		require.NoError(t, err)
		assert.NotNil(t, decoded)
		assert.Equal(t, int32(123), decoded["id"])
		assert.Nil(t, decoded["description"])
	})

	t.Run("should use string fallback for unknown types", func(t *testing.T) {
		relationColumns := []RelationColumn{
			{Name: "unknown_col", DataType: 999999}, // Unknown OID
			{Name: "description", DataType: pgtype.TextOID},
		}

		decoded, err := d.DecodeWithColumn(relationColumns)
		require.NoError(t, err)
		assert.Equal(t, "123", decoded["unknown_col"]) // Fallback to string
	})
}

func TestDecodeTextColumnData_JSON(t *testing.T) {
	t.Run("should keep JSON null as raw text instead of nil", func(t *testing.T) {
		for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
			val, err := decodeTextColumnData([]byte("null"), oid)
			require.NoError(t, err)
			assert.Equal(t, json.RawMessage("null"), val)
		}
	})

	t.Run("should preserve JSON string quotes", func(t *testing.T) {
		val, err := decodeTextColumnData([]byte(`"hahaha"`), pgtype.JSONBOID)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"hahaha"`), val)
	})

	t.Run("should preserve object text and big integers", func(t *testing.T) {
		val, err := decodeTextColumnData([]byte(`{"n": 9007199254740993}`), pgtype.JSONBOID)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"n": 9007199254740993}`), val)
	})

	t.Run("should round-trip through json.Marshal verbatim", func(t *testing.T) {
		val, err := decodeTextColumnData([]byte(`{"a": 1}`), pgtype.JSONBOID)
		require.NoError(t, err)
		marshaled, err := json.Marshal(val)
		require.NoError(t, err)
		assert.Equal(t, `{"a":1}`, string(marshaled))
	})

	t.Run("should still decode non-JSON types via pgtype", func(t *testing.T) {
		val, err := decodeTextColumnData([]byte("42"), pgtype.Int8OID)
		require.NoError(t, err)
		assert.Equal(t, int64(42), val)
	})
}

func TestData_DecodeWithColumn_JSONNull(t *testing.T) {
	// 构造两列：jsonb 列存 JSON null（文本 't'），text 列为 SQL NULL（'n'）
	buf := []byte{'D'}
	colCount := make([]byte, 2)
	binary.BigEndian.PutUint16(colCount, 2)
	buf = append(buf, colCount...)

	buf = append(buf, DataTypeText)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 4)
	buf = append(buf, lenBuf...)
	buf = append(buf, []byte("null")...)

	buf = append(buf, DataTypeNull)

	d, err := NewData(buf, 'D', 0)
	require.NoError(t, err)

	relationColumns := []RelationColumn{
		{Name: "meta", DataType: pgtype.JSONBOID},
		{Name: "note", DataType: pgtype.TextOID},
	}

	decoded, err := d.DecodeWithColumn(relationColumns)
	require.NoError(t, err)
	// JSON null 必须保留为 RawMessage，不能坍缩成 SQL NULL 的 nil
	assert.Equal(t, json.RawMessage("null"), decoded["meta"])
	assert.Nil(t, decoded["note"])
}
