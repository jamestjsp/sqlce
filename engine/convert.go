// Package engine provides data conversion and row reading for SQL CE databases.
package engine

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"time"
	"unicode/utf16"

	"github.com/jamestjat/sqlce/format"
)

// ParseGUID converts 16 bytes in SQL Server mixed-endian format to a standard
// GUID string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
//
// SQL Server GUID byte order:
//
//	bytes 0-3:  Data1 (little-endian u32)
//	bytes 4-5:  Data2 (little-endian u16)
//	bytes 6-7:  Data3 (little-endian u16)
//	bytes 8-15: Data4 (big-endian, raw bytes)
func ParseGUID(b []byte) (string, error) {
	if len(b) < 16 {
		return "", fmt.Errorf("GUID requires 16 bytes, got %d", len(b))
	}
	d1 := binary.LittleEndian.Uint32(b[0:4])
	d2 := binary.LittleEndian.Uint16(b[4:6])
	d3 := binary.LittleEndian.Uint16(b[6:8])
	return fmt.Sprintf("%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		d1, d2, d3,
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	), nil
}

// SQL Server / SQL CE datetime epoch: January 1, 1900 00:00:00 UTC.
var datetimeEpoch = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// ParseDateTime converts an 8-byte SQL CE datetime to a Go time.Time.
//
// Format: two signed int32 little-endian values:
//
//	bytes 0-3: days since 1900-01-01 (negative for pre-1900)
//	bytes 4-7: ticks since midnight (1 tick = 1/300th second)
func ParseDateTime(b []byte) (time.Time, error) {
	if len(b) < 8 {
		return time.Time{}, fmt.Errorf("datetime requires 8 bytes, got %d", len(b))
	}
	days := int32(binary.LittleEndian.Uint32(b[0:4]))
	ticks := int32(binary.LittleEndian.Uint32(b[4:8]))

	t := datetimeEpoch.AddDate(0, 0, int(days))
	ms := int64(ticks) * 1000 / 300
	t = t.Add(time.Duration(ms) * time.Millisecond)

	return t, nil
}

// ConvertValue converts raw bytes to a Go value based on the SQL CE type.
func ConvertValue(data []byte, typeID uint16) (any, error) {
	le := binary.LittleEndian

	switch typeID {
	case format.TypeBit:
		if len(data) < 1 {
			return nil, fmt.Errorf("bit: need 1 byte, got %d", len(data))
		}
		return data[0] != 0, nil

	case format.TypeTinyInt:
		if len(data) < 1 {
			return nil, fmt.Errorf("tinyint: need 1 byte, got %d", len(data))
		}
		return data[0], nil

	case format.TypeSmallInt:
		if len(data) < 2 {
			return nil, fmt.Errorf("smallint: need 2 bytes, got %d", len(data))
		}
		return int16(le.Uint16(data)), nil

	case format.TypeInt:
		if len(data) < 4 {
			return nil, fmt.Errorf("int: need 4 bytes, got %d", len(data))
		}
		return int32(le.Uint32(data)), nil

	case format.TypeBigInt:
		if len(data) < 8 {
			return nil, fmt.Errorf("bigint: need 8 bytes, got %d", len(data))
		}
		return int64(le.Uint64(data)), nil

	case format.TypeReal:
		if len(data) < 4 {
			return nil, fmt.Errorf("real: need 4 bytes, got %d", len(data))
		}
		return math.Float32frombits(le.Uint32(data)), nil

	case format.TypeFloat:
		if len(data) < 8 {
			return nil, fmt.Errorf("float: need 8 bytes, got %d", len(data))
		}
		return math.Float64frombits(le.Uint64(data)), nil

	case format.TypeMoney:
		if len(data) < 8 {
			return nil, fmt.Errorf("money: need 8 bytes, got %d", len(data))
		}
		return int64(le.Uint64(data)), nil

	case format.TypeDatetime:
		return ParseDateTime(data)

	case format.TypeUniqueIdentifier:
		return ParseGUID(data)

	case format.TypeNVarchar, format.TypeNChar:
		if isUTF16LE(data) {
			return decodeUTF16LE(data), nil
		}
		return string(data), nil

	case format.TypeNText:
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil

	case format.TypeBinary, format.TypeVarBinary, format.TypeImage, format.TypeRowVersion:
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil

	case format.TypeNumeric:
		return parseNumeric(data)

	default:
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	}
}

// parseNumeric decodes a 19-byte SQL CE numeric value.
// Format: [precision u8][scale u8][sign u8 (1=pos,0=neg)][16-byte uint128 LE]
func parseNumeric(data []byte) (string, error) {
	if len(data) != 19 {
		return "", fmt.Errorf("numeric: expected 19 bytes, got %d", len(data))
	}

	scale := int(data[1])
	sign := data[2]

	val := new(big.Int)
	be := make([]byte, 16)
	for i := 0; i < 16; i++ {
		be[i] = data[18-i]
	}
	val.SetBytes(be)

	s := val.String()

	if scale > 0 {
		if len(s) <= scale {
			s = fmt.Sprintf("%0*s", scale+1, s)
		}
		s = s[:len(s)-scale] + "." + s[len(s)-scale:]
	}

	if sign == 0 {
		s = "-" + s
	}

	return s, nil
}

func isUTF16LE(data []byte) bool {
	if len(data) < 2 || len(data)%2 != 0 {
		return false
	}
	zeros := 0
	for i := 1; i < len(data); i += 2 {
		if data[i] == 0 && data[i-1] != 0 {
			zeros++
		}
	}
	return zeros > len(data)/4
}

// decodeUTF16LE converts a UTF-16LE byte slice to a Go string.
func decodeUTF16LE(b []byte) string {
	if len(b) < 2 {
		return ""
	}
	// Trim trailing null characters
	for len(b) >= 2 && b[len(b)-1] == 0 && b[len(b)-2] == 0 {
		b = b[:len(b)-2]
	}
	u16s := make([]uint16, len(b)/2)
	for i := range u16s {
		u16s[i] = binary.LittleEndian.Uint16(b[i*2:])
	}
	return string(utf16.Decode(u16s))
}
