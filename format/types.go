package format

import (
	"reflect"
	"time"
)

// SQL CE internal type IDs as discovered from catalog record analysis.
const (
	TypeTinyInt          uint16 = 0x01
	TypeSmallInt         uint16 = 0x02
	TypeInt              uint16 = 0x03
	TypeBigInt           uint16 = 0x04
	TypeFloat            uint16 = 0x05
	TypeReal             uint16 = 0x06
	TypeMoney            uint16 = 0x08
	TypeBit              uint16 = 0x0B
	TypeNChar            uint16 = 0x1E
	TypeNVarchar         uint16 = 0x1F
	TypeDatetime         uint16 = 0x40
	TypeImage            uint16 = 0x41
	TypeBinary           uint16 = 0x3D
	TypeVarBinary        uint16 = 0x3F
	TypeNText            uint16 = 0x64
	TypeUniqueIdentifier uint16 = 0x65
	TypeNumeric          uint16 = 0x6C
	TypeRowVersion       uint16 = 0x69
)

// TypeInfo describes a SQL CE data type and its Go equivalent.
type TypeInfo struct {
	// ID is the SQL CE internal type identifier.
	ID uint16

	// Name is the SQL CE type name (e.g. "int", "nvarchar").
	Name string

	// GoType is the Go reflect.Type for this column type.
	GoType reflect.Type

	// FixedSize is the byte size for fixed-length types, or 0 for variable-length.
	FixedSize int

	// IsVariable indicates whether the type has variable length (nvarchar, varbinary, etc.).
	IsVariable bool
}

var (
	typeBool    = reflect.TypeOf(false)
	typeUint8   = reflect.TypeOf(uint8(0))
	typeInt16   = reflect.TypeOf(int16(0))
	typeInt32   = reflect.TypeOf(int32(0))
	typeInt64   = reflect.TypeOf(int64(0))
	typeFloat32 = reflect.TypeOf(float32(0))
	typeFloat64 = reflect.TypeOf(float64(0))
	typeString  = reflect.TypeOf("")
	typeBytes   = reflect.TypeOf([]byte(nil))
	typeTime    = reflect.TypeOf(time.Time{})
)

// typeRegistry maps SQL CE type IDs to TypeInfo.
var typeRegistry = map[uint16]TypeInfo{
	TypeTinyInt:          {TypeTinyInt, "tinyint", typeUint8, 1, false},
	TypeSmallInt:         {TypeSmallInt, "smallint", typeInt16, 2, false},
	TypeInt:              {TypeInt, "int", typeInt32, 4, false},
	TypeBigInt:           {TypeBigInt, "bigint", typeInt64, 8, false},
	TypeFloat:            {TypeFloat, "float", typeFloat64, 8, false},
	TypeReal:             {TypeReal, "real", typeFloat32, 4, false},
	TypeMoney:            {TypeMoney, "money", typeInt64, 8, false},
	TypeBit:              {TypeBit, "bit", typeBool, 0, false},
	TypeNVarchar:         {TypeNVarchar, "nvarchar", typeString, 0, true},
	TypeNChar:            {TypeNChar, "nchar", typeString, 0, true},
	TypeDatetime:         {TypeDatetime, "datetime", typeTime, 8, false},
	TypeImage:            {TypeImage, "image", typeBytes, 16, false},
	TypeBinary:           {TypeBinary, "binary", typeBytes, 0, true},
	TypeVarBinary:        {TypeVarBinary, "varbinary", typeBytes, 0, true},
	TypeNText:            {TypeNText, "ntext", typeString, 16, false},
	TypeUniqueIdentifier: {TypeUniqueIdentifier, "uniqueidentifier", typeString, 16, false},
	TypeNumeric:          {TypeNumeric, "numeric", typeString, 19, false},
	TypeRowVersion:       {TypeRowVersion, "rowversion", typeBytes, 8, false},
}

// LookupType returns the TypeInfo for a SQL CE type ID.
// Returns a zero TypeInfo with the ID set if the type is unknown.
func LookupType(typeID uint16) TypeInfo {
	if info, ok := typeRegistry[typeID]; ok {
		return info
	}
	return TypeInfo{ID: typeID, Name: "unknown", GoType: typeBytes, IsVariable: true}
}

// TypeName returns the SQL CE type name for the given type ID.
func TypeName(typeID uint16) string {
	return LookupType(typeID).Name
}
