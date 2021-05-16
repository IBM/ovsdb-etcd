package libovsdb

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

type Schemas map[string]*DatabaseSchema

func (schemas *Schemas) AddFromFile(path string) error {
	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	data, _ := ioutil.ReadAll(jsonFile)
	var databaseSchema DatabaseSchema
	err = json.Unmarshal(data, &databaseSchema)
	if err != nil {
		return err
	}
	schemas.Add(&databaseSchema)
	return nil
}

func (schemas *Schemas) Add(databaseSchema *DatabaseSchema) {
	(*schemas)[databaseSchema.Name] = databaseSchema
}

// DatabaseSchema is a database schema according to RFC7047
type DatabaseSchema struct {
	Name    string                 `json:"name"`
	Version string                 `json:"version"`
	Tables  map[string]TableSchema `json:"tables"`
	Checksm string                 `json:"checksm,omitempty"`
}

// GetColumn returns a Column Schema for a given table and column name
func (schema DatabaseSchema) GetColumn(tableName, columnName string) (*ColumnSchema, error) {
	table, ok := schema.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found in schema %s", tableName)
	}
	if columnName == "_uuid" {
		return &ColumnSchema{
			Type: TypeUUID,
		}, nil
	}
	column, ok := table.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("column not found in schema %s", columnName)
	}
	return column, nil
}

// Print will print the contents of the DatabaseSchema
func (schema DatabaseSchema) Print(w io.Writer) {
	fmt.Fprintf(w, "%s, (%s)\n", schema.Name, schema.Version)
	for table, tableSchema := range schema.Tables {
		fmt.Fprintf(w, "\t %s\n", table)
		for column, columnSchema := range tableSchema.Columns {
			fmt.Fprintf(w, "\t\t %s => %s\n", column, columnSchema)
		}
	}
}

// Basic validation for operations against Database Schema
func (schema DatabaseSchema) validateOperations(operations ...Operation) bool {
	for _, op := range operations {
		table, ok := schema.Tables[op.Table]
		if ok {
			for column := range op.Row {
				if _, ok := table.Columns[column]; !ok {
					if column != "_uuid" && column != "_version" {
						return false
					}
				}
			}
			for _, row := range op.Rows {
				for column := range row {
					if _, ok := table.Columns[column]; !ok {
						if column != "_uuid" && column != "_version" {
							return false
						}
					}
				}
			}
			for _, column := range op.Columns {
				if _, ok := table.Columns[column]; !ok {
					if column != "_uuid" && column != "_version" {
						return false
					}
				}
			}
		} else {
			return false
		}
	}
	return true
}

func (schema *DatabaseSchema) String() string {
	buf, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal: %s", err))
	}
	return string(buf)
}

// TableSchema is a table schema according to RFC7047
type TableSchema struct {
	Columns map[string]*ColumnSchema `json:"columns"`
	Indexes [][]string               `json:"indexes,omitempty"`
	MaxRows int                      `json:"maxRows,omitempty"`
	IsRoot  bool                     `json:"isRoot,omitempty"`
}

/*RFC7047 defines some atomic-types (e.g: integer, string, etc). However, the Column's type
can also hold other more complex types such as set, enum and map. The way to determine the type
depends on internal, not directly marshallable fields. Therefore, in order to simplify the usage
of this library, we define an ExtendedType that includes all possible column types (including
atomic fields).
*/

//ExtendedType includes atomic types as defined in the RFC plus Enum, Map and Set
type ExtendedType = string

// RefType is used to define the possible RefTypes
type RefType = string

const (
	//Unlimited is used to express unlimited "Max"
	Unlimited int = -1

	//Strong RefType
	Strong RefType = "strong"
	//Weak RefType
	Weak RefType = "weak"

	//ExtendedType associated with Atomic Types

	//TypeInteger is equivalent to 'int'
	TypeInteger ExtendedType = "integer"
	//TypeReal is equivalent to 'float64'
	TypeReal ExtendedType = "real"
	//TypeBoolean is equivalent to 'bool'
	TypeBoolean ExtendedType = "boolean"
	//TypeString is equivalent to 'string'
	TypeString ExtendedType = "string"
	//TypeUUID is equivalent to 'libovsdb.UUID'
	TypeUUID ExtendedType = "uuid"

	//Extended Types used to summarize the interal type of the field.

	//TypeEnum is an enumerator of type defined by Key.Type
	TypeEnum ExtendedType = "enum"
	//TypeMap is a map whose type depend on Key.Type and Value.Type
	TypeMap ExtendedType = "map"
	//TypeSet is a set whose type depend on Key.Type
	TypeSet ExtendedType = "set"
)

// ColumnSchema is a column schema according to RFC7047
type ColumnSchema struct {
	// According to RFC7047, "type" field can be, either an <atomic-type>
	// Or a ColumnTypeObject defined below. To try to simplify the usage, the
	// json message will be parsed manually and Type will indicate the "extended"
	// type. Depending on its value, more information may be available in TypeObj.
	// E.g: If Type == TypeEnum, TypeObj.Key.Enum contains the possible values
	Type      ExtendedType `json:"type"`
	TypeObj   *ColumnType  `json:"typeObj,omitempty"`
	Ephemeral *bool        `json:"ephemeral,omitempty"`
	Mutable   *bool        `json:"mutable,omitempty"`
}

func NewColumnSchema() *ColumnSchema {
	t := true
	f := false
	return &ColumnSchema{
		Ephemeral: &f,
		Mutable:   &t,
	}
}

// ColumnType is a type object as per RFC7047
type ColumnType struct {
	Key   *BaseType `json:"key,omitempty"`
	Value *BaseType `json:"value,omitempty"`
	Min   int
	// Unlimited is expressed by the const value Unlimited (-1)
	Max int
}

// BaseType is a base-type structure as per RFC7047
type BaseType struct {
	Type string `json:"type"`
	// Enum will be parsed manually and set to a slice
	// of possible values. They must be type-asserted to the
	// corret type depending on the Type field
	Enum       *OvsSet `json:"enum,omitempty"`
	MinReal    float64 `json:"minReal,omitempty"`
	MaxReal    float64 `json:"maxReal,omitempty"`
	MinInteger int     `json:"minInteger,omitempty"`
	MaxInteger int     `json:"maxInteger,omitempty"`
	MinLength  int     `json:"minLength,omitempty"` /* string */
	MaxLength  int     `json:"maxLength,omitempty"` /* string */
	RefTable   string  `json:"refTable,omitempty"`  /* UUIDs */
	RefType    RefType `json:"refType,omitempty"`
}

// String returns a string representation of the (native) column type
func (column *ColumnSchema) String() string {
	var flags []string
	var flagStr string
	var typeStr string
	if *column.Ephemeral {
		flags = append(flags, "E")
	}
	if *column.Mutable {
		flags = append(flags, "M")
	}
	if len(flags) > 0 {
		flagStr = fmt.Sprintf("[%s]", strings.Join(flags, ","))
	}

	switch column.Type {
	case TypeInteger, TypeReal, TypeBoolean, TypeString:
		typeStr = string(column.Type)
	case TypeUUID:
		if column.TypeObj != nil && column.TypeObj.Key != nil {
			typeStr = fmt.Sprintf("uuid [%s (%s)]", column.TypeObj.Key.RefTable, column.TypeObj.Key.RefType)
		} else {
			typeStr = "uuid"
		}

	case TypeEnum:
		typeStr = fmt.Sprintf("enum (type: %s): %v", column.TypeObj.Key.Type, column.TypeObj.Key.Enum)
	case TypeMap:
		typeStr = fmt.Sprintf("[%s]%s", column.TypeObj.Key.Type, column.TypeObj.Value.Type)
	case TypeSet:
		var keyStr string
		if column.TypeObj.Key.Type == TypeUUID {
			keyStr = fmt.Sprintf(" [%s (%s)]", column.TypeObj.Key.RefTable, column.TypeObj.Key.RefType)
		} else {
			keyStr = string(column.TypeObj.Key.Type)
		}
		typeStr = fmt.Sprintf("[]%s (min: %d, max: %d)", keyStr, column.TypeObj.Min, column.TypeObj.Max)
	default:
		panic(fmt.Sprintf("Unsupported type %s", column.Type))
	}

	return strings.Join([]string{typeStr, flagStr}, " ")
}

// UnmarshalJSON unmarshalls a json-formatted column
func (column *ColumnSchema) UnmarshalJSON(data []byte) error {
	// ColumnJSON represents the known json values for a Column
	type ColumnJSON struct {
		TypeRawMsg json.RawMessage `json:"type"`
		Ephemeral  *bool           `json:"ephemeral,omitempty"`
		Mutable    *bool           `json:"mutable,omitempty"`
	}
	var colJSON ColumnJSON

	// Unmarshall known keys
	if err := json.Unmarshal(data, &colJSON); err != nil {
		return fmt.Errorf("cannot parse column object %s", err)
	}

	if colJSON.Ephemeral != nil && *colJSON.Ephemeral {
		column.Ephemeral = colJSON.Ephemeral
	}
	if colJSON.Mutable != nil && *colJSON.Mutable {
		column.Mutable = colJSON.Mutable
	}

	// 'type' can be a string or an object, let's figure it out
	var typeString string
	if err := json.Unmarshal(colJSON.TypeRawMsg, &typeString); err == nil {
		if !isAtomicType(typeString) {
			return fmt.Errorf("schema contains unknown atomic type %s", typeString)
		}
		// This was an easy one. Use the string as our 'extended' type
		column.Type = typeString
		return nil
	}

	// 'type' can be an object defined as:
	// "key": <base-type>                 required
	// "value": <base-type>               optional
	// "min": <integer>                   optional (default: 1)
	// "max": <integer> or "unlimited"    optional (default: 1)
	column.TypeObj = &ColumnType{
		Key:   &BaseType{},
		Value: nil,
		Max:   1,
		Min:   1,
	}

	// ColumnTypeJSON is used to dynamically decode the ColumnType
	type ColumnTypeJSON struct {
		KeyRawMsg   *json.RawMessage `json:"key,omitempty"`
		ValueRawMsg *json.RawMessage `json:"value,omitempty"`
		Min         int              `json:"min,omitempty"`
		MaxRawMsg   *json.RawMessage `json:"max,omitempty"`
	}
	colTypeJSON := ColumnTypeJSON{
		Min: 1,
	}

	if err := json.Unmarshal(colJSON.TypeRawMsg, &colTypeJSON); err != nil {
		return fmt.Errorf("cannot parse type object: %s", err)
	}

	// Now we have to unmarshall some fields manually because they can store
	// values of different types. Also, in order to really know what native
	// type can store a value of this column, the RFC defines some logic based
	// on the values of 'type'. So, in addition to manually unmarshalling, let's
	// figure out what is the real native type and store it in column.Type for
	// ease of use.

	// 'max' can be an integer or the string "unlimmited". To simplify, use -1
	// as unlimited
	if colTypeJSON.MaxRawMsg != nil {
		var maxString string
		if err := json.Unmarshal(*colTypeJSON.MaxRawMsg, &maxString); err == nil {
			if maxString == "unlimited" {
				column.TypeObj.Max = Unlimited
			} else {
				return fmt.Errorf("unknown max value %s", maxString)
			}
		} else if err := json.Unmarshal(*colTypeJSON.MaxRawMsg, &column.TypeObj.Max); err != nil {
			return fmt.Errorf("cannot parse max field: %s", err)
		}
	}
	column.TypeObj.Min = colTypeJSON.Min

	// 'key' and 'value' can, themselves, be a string or a BaseType.
	// key='<atomic_type>' is equivalent to 'key': {'type': '<atomic_type>'}
	// To simplify things a bit, we'll translate the former to the latter

	if err := json.Unmarshal(*colTypeJSON.KeyRawMsg, &column.TypeObj.Key.Type); err != nil {
		if err := json.Unmarshal(*colTypeJSON.KeyRawMsg, column.TypeObj.Key); err != nil {
			return fmt.Errorf("cannot parse key object: %s", err)
		}
		if err := column.TypeObj.Key.parseEnum(*colTypeJSON.KeyRawMsg); err != nil {
			return err
		}
	}

	if !isAtomicType(column.TypeObj.Key.Type) {
		return fmt.Errorf("schema contains unknown atomic type %s", column.TypeObj.Key.Type)
	}

	// 'value' is optional. If it exists, we know the real native type is a map
	if colTypeJSON.ValueRawMsg != nil {
		column.TypeObj.Value = &BaseType{}
		if err := json.Unmarshal(*colTypeJSON.ValueRawMsg, &column.TypeObj.Value.Type); err != nil {
			if err := json.Unmarshal(*colTypeJSON.ValueRawMsg, &column.TypeObj.Value); err != nil {
				return fmt.Errorf("cannot parse value object: %s", err)
			}
			if err := column.TypeObj.Value.parseEnum(*colTypeJSON.ValueRawMsg); err != nil {
				return err
			}
		}
		if !isAtomicType(column.TypeObj.Value.Type) {
			return fmt.Errorf("schema contains unknown atomic type %s", column.TypeObj.Key.Type)
		}
	}

	// Technially, we have finished unmarshalling. But let's finish infering the native
	if column.TypeObj.Value != nil {
		column.Type = TypeMap
	} else if column.TypeObj.Min != 1 || column.TypeObj.Max != 1 {
		column.Type = TypeSet
	} else if column.TypeObj.Key.Enum != nil && len(column.TypeObj.Key.Enum.GoSet) > 0 {
		column.Type = TypeEnum
	} else {
		column.Type = column.TypeObj.Key.Type
	}
	return nil
}

// parseEnum decodes the enum field and populates the BaseType.Enum field
func (bt *BaseType) parseEnum(rawData json.RawMessage) error {
	// EnumJSON is used to dynamically decode the Enum values
	type EnumJSON struct {
		Enum interface{} `json:"enum,omitempty"`
	}
	var enumJSON EnumJSON

	if err := json.Unmarshal(rawData, &enumJSON); err != nil {
		return fmt.Errorf("cannot parse enum object: %s (%s)", string(rawData), err)
	}
	// enum is optional
	if enumJSON.Enum == nil {
		return nil
	}

	// 'enum' is a list or a single element representing a list of exactly one element
	switch enumJSON.Enum.(type) {
	case []interface{}:
		// it's an OvsSet
		oSet := enumJSON.Enum.([]interface{})
		innerSet := oSet[1].([]interface{})
		bt.Enum = &OvsSet{GoSet: make([]interface{}, len(innerSet))}
		for k, val := range innerSet {
			bt.Enum.GoSet[k] = val
		}
	default:
		bt.Enum = &OvsSet{GoSet: []interface{}{enumJSON.Enum}}
	}
	return nil
}

func isAtomicType(atype string) bool {
	switch atype {
	case TypeInteger, TypeReal, TypeBoolean, TypeString, TypeUUID:
		return true
	default:
		return false
	}
}

/* default value generation */
func (columnSchema *ColumnSchema) Default() interface{} {
	switch columnSchema.Type {
	case TypeInteger:
		return int(0)
	case TypeReal:
		return float64(0)
	case TypeBoolean:
		return false
	case TypeString:
		return ""
	case TypeUUID:
		return UUID{"00000000-0000-0000-0000-000000000000"}
	case TypeEnum:
		switch columnSchema.TypeObj.Key.Type {
		case TypeString:
			return ""
		default:
			panic(fmt.Sprintf("Unsupported enum type %s", columnSchema.TypeObj.Key.Type))
		}
	case TypeSet:
		return OvsSet{}
	case TypeMap:
		return OvsMap{}
	default:
		panic(fmt.Sprintf("Unsupported type %s", columnSchema.Type))
	}
}

func (tableSchema *TableSchema) Default(row *map[string]interface{}) {
	for column, columnSchema := range tableSchema.Columns {
		if _, ok := (*row)[column]; !ok {
			(*row)[column] = columnSchema.Default()
		}
	}
}

func (databaseSchema *DatabaseSchema) Default(table string, row *map[string]interface{}) {
	tableSchema, ok := databaseSchema.Tables[table]
	if !ok {
		panic(fmt.Sprintf("Missing schema for table %s", table))
	}
	tableSchema.Default(row)
}

func (schemas *Schemas) Default(dbname, table string, row *map[string]interface{}) {
	databaseSchema, ok := (*schemas)[dbname]
	if !ok {
		panic(fmt.Sprintf("Missing schema for database %s", table))
	}
	databaseSchema.Default(table, row)
}

/* convert types */
func UnmarshalInteger(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to int
	json.Unmarshal(data, &to)
	return to, nil
}

func UnmarshalReal(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to float64
	json.Unmarshal(data, &to)
	return to, nil
}

func UnmarshalString(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to string
	json.Unmarshal(data, &to)
	return to, nil
}

func UnmarshalBoolean(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to bool
	json.Unmarshal(data, &to)
	return to, nil
}

func UnmarshalUUID(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to UUID
	json.Unmarshal(data, &to)
	return to, nil
}

func UnmarshalEnum(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to interface{}
	json.Unmarshal(data, &to)
	return to, nil
}

func UnmarshalSet(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to OvsSet
	json.Unmarshal(data, &to)
	return to, nil
}

func UnmarshalMap(from interface{}) (interface{}, error) {
	data, err := json.Marshal(from)
	if err != nil {
		return nil, err
	}
	var to OvsMap
	json.Unmarshal(data, &to)
	return to, nil
}

func (columnSchema *ColumnSchema) Unmarshal(from interface{}) (interface{}, error) {
	switch columnSchema.Type {
	case TypeInteger:
		return UnmarshalInteger(from)
	case TypeReal:
		return UnmarshalReal(from)
	case TypeBoolean:
		return UnmarshalBoolean(from)
	case TypeString:
		return UnmarshalString(from)
	case TypeUUID:
		return UnmarshalUUID(from)
	case TypeEnum:
		return UnmarshalEnum(from)
	case TypeSet:
		return UnmarshalSet(from)
	case TypeMap:
		return UnmarshalMap(from)
	default:
		panic(fmt.Sprintf("unsupported type %s", columnSchema.Type))
	}
}

func (tableSchema *TableSchema) Unmarshal(row *map[string]interface{}) error {
	for column, columnSchema := range tableSchema.Columns {
		if _, ok := (*row)[column]; ok {
			to, err := columnSchema.Unmarshal((*row)[column])
			if err != nil {
				return fmt.Errorf("[column %s] %s", column, err.Error())
			}
			(*row)[column] = to
		}
	}
	return nil
}

func (databaseSchema *DatabaseSchema) Unmarshal(table string, row *map[string]interface{}) error {
	tableSchema, ok := databaseSchema.Tables[table]
	if !ok {
		return fmt.Errorf("missing schema for table %s", table)
	}
	err := tableSchema.Unmarshal(row)
	if err != nil {
		return fmt.Errorf("[table %s] %s", table, err)
	}
	return nil
}

func (schemas *Schemas) Unmarshal(dbname, table string, row *map[string]interface{}) error {
	databaseSchema, ok := (*schemas)[dbname]
	if !ok {
		return fmt.Errorf("missing schema for database %s", dbname)
	}
	err := databaseSchema.Unmarshal(table, row)
	if err != nil {
		return fmt.Errorf("[database %s] %s", dbname, err)
	}
	return nil
}

/* lookup */
func (tableSchema *TableSchema) LookupColumn(column string) (*ColumnSchema, error) {
	columnSchema, ok := tableSchema.Columns[column]
	if !ok {
		return nil, fmt.Errorf("Missing schema for column %s", column)
	}
	return columnSchema, nil
}

func (databaseSchema *DatabaseSchema) LookupColumn(table, column string) (*ColumnSchema, error) {
	tableSchema, ok := databaseSchema.Tables[table]
	if !ok {
		return nil, fmt.Errorf("Missing schema for table %s", table)
	}
	return tableSchema.LookupColumn(column)
}

func (schemas *Schemas) LookupColumn(dbname, table, column string) (*ColumnSchema, error) {
	databaseSchema, ok := (*schemas)[dbname]
	if !ok {
		return nil, fmt.Errorf("Missing schema for database %s", dbname)
	}
	return databaseSchema.LookupColumn(table, column)
}

func (databaseSchema *DatabaseSchema) LookupTable(table string) (*TableSchema, error) {
	tableSchema, ok := databaseSchema.Tables[table]
	if !ok {
		return nil, fmt.Errorf("Missing schema for table %s", table)
	}
	return &tableSchema, nil
}

func (schemas *Schemas) LookupTable(dbname, table string) (*TableSchema, error) {
	databaseSchema, ok := (*schemas)[dbname]
	if !ok {
		return nil, fmt.Errorf("Missing schema for database %s", dbname)
	}
	return databaseSchema.LookupTable(table)
}

/* validate */
func (baseType *BaseType) ValidateInteger(value interface{}) error {
	_, ok := value.(int)
	if !ok {
		return fmt.Errorf("expected integer: %+v", value)
	}
	return nil
}

func (baseType *BaseType) ValidateReal(value interface{}) error {
	_, ok := value.(float64)
	if !ok {
		return fmt.Errorf("expected integer: %+v", value)
	}
	return nil
}

func (baseType *BaseType) ValidateBoolean(value interface{}) error {
	_, ok := value.(bool)
	if !ok {
		return fmt.Errorf("expected integer: %+v", value)
	}
	return nil
}

func (baseType *BaseType) ValidateString(value interface{}) error {
	typeval, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected integer: %+v", value)
	}
	if baseType.Enum == nil {
		return nil
	}
	for _, v := range baseType.Enum.GoSet {
		enumval, ok := v.(string)
		if !ok {
			return fmt.Errorf("expected string: %+v", value)
		}
		if typeval == enumval {
			return nil
		}
	}
	return fmt.Errorf("enum value is not valid: %+v", value)
}

func (baseType *BaseType) ValidateUUID(value interface{}) error {
	_, ok := value.(UUID)
	if !ok {
		return fmt.Errorf("expected integer: %+v", value)
	}
	return nil
}

func (baseType *BaseType) Validate(value interface{}) error {
	if baseType == nil {
		panic(fmt.Sprintf("nil base type"))
	}
	switch baseType.Type {
	case TypeInteger:
		return baseType.ValidateInteger(value)
	case TypeString:
		return baseType.ValidateString(value)
	case TypeBoolean:
		return baseType.ValidateBoolean(value)
	case TypeReal:
		return baseType.ValidateReal(value)
	case TypeUUID:
		return baseType.ValidateUUID(value)
	default:
		panic(fmt.Sprintf("unsupported value type %s", baseType.Type))
	}
}

func (columnSchema *ColumnSchema) ValidateInteger(value interface{}) error {
	_, ok := value.(int)
	if !ok {
		return fmt.Errorf("expected integer: %+v", value)
	}
	return nil
}

func (columnSchema *ColumnSchema) ValidateReal(value interface{}) error {
	_, ok := value.(float64)
	if !ok {
		return fmt.Errorf("expected real: %+v", value)
	}
	return nil
}

func (columnSchema *ColumnSchema) ValidateBoolean(value interface{}) error {
	_, ok := value.(bool)
	if !ok {
		return fmt.Errorf("expected boolean: %+v", value)
	}
	return nil
}

func (columnSchema *ColumnSchema) ValidateString(value interface{}) error {
	_, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string: %+v", value)
	}
	return nil
}

func (columnSchema *ColumnSchema) ValidateUUID(value interface{}) error {
	_, ok := value.(UUID)
	if !ok {
		return fmt.Errorf("expected uuid: %+v", value)
	}
	return nil
}

func (columnSchema *ColumnSchema) ValidateEnum(value interface{}) error {
	return columnSchema.TypeObj.Key.Validate(value)
}

func (columnSchema *ColumnSchema) ValidateSet(value interface{}) error {
	setval, ok := value.(OvsSet)
	if !ok {
		return fmt.Errorf("expected set: %+v", value)
	}

	l := len(setval.GoSet)
	if l < columnSchema.TypeObj.Min {
		return fmt.Errorf("set breaks min limit: %+v", value)
	}
	if columnSchema.TypeObj.Max != Unlimited && columnSchema.TypeObj.Max < l {
		return fmt.Errorf("set breaks max limit: %+v", value)
	}
	for _, val := range setval.GoSet {
		err := columnSchema.TypeObj.Value.Validate(val)
		if err != nil {
			return fmt.Errorf("set inner value invalid: %s", err.Error())
		}
	}
	return nil
}

func (columnSchema *ColumnSchema) ValidateMap(value interface{}) error {
	mapval, ok := value.(OvsMap)
	if !ok {
		return fmt.Errorf("expected map: %+v", value)
	}

	l := 0
	for key, val := range mapval.GoMap {
		l++
		err := columnSchema.TypeObj.Key.Validate(key)
		if err != nil {
			return fmt.Errorf("map key invalid: %s", err.Error())
		}
		err = columnSchema.TypeObj.Value.Validate(val)
		if err != nil {
			return fmt.Errorf("map value invalid: %s", err.Error())
		}
	}

	if l < columnSchema.TypeObj.Min {
		return fmt.Errorf("map breaks min limit: %+v", value)
	}
	if columnSchema.TypeObj.Max != Unlimited && columnSchema.TypeObj.Max < l {
		return fmt.Errorf("map breaks max limit: %+v", value)
	}

	return nil
}

func (columnSchema *ColumnSchema) Validate(value interface{}) error {
	if columnSchema == nil {
		panic(fmt.Sprintf("nil column schema"))
	}
	switch columnSchema.Type {
	case TypeInteger:
		return columnSchema.ValidateInteger(value)
	case TypeReal:
		return columnSchema.ValidateReal(value)
	case TypeBoolean:
		return columnSchema.ValidateBoolean(value)
	case TypeString:
		return columnSchema.ValidateString(value)
	case TypeUUID:
		return columnSchema.ValidateUUID(value)
	case TypeEnum:
		return columnSchema.ValidateEnum(value)
	case TypeSet:
		return columnSchema.ValidateSet(value)
	case TypeMap:
		return columnSchema.ValidateMap(value)
	default:
		panic(fmt.Sprintf("unsupported type %s", columnSchema.Type))
	}
}

func (tableSchema *TableSchema) Validate(row *map[string]interface{}) error {
	for column, value := range *row {
		if column == "_uuid" || column == "_version" {
			continue
		}
		columnSchema, ok := tableSchema.Columns[column]
		if !ok {
			return fmt.Errorf("missing schema for column %s", column)
		}
		err := columnSchema.Validate(value)
		if err != nil {
			return fmt.Errorf("[column %s] %s", column, err.Error())
		}
	}
	return nil
}

func (databaseSchema *DatabaseSchema) Validate(table string, row *map[string]interface{}) error {
	tableSchema, ok := databaseSchema.Tables[table]
	if !ok {
		return fmt.Errorf("did not find schema for table %s", table)
	}
	err := tableSchema.Validate(row)
	if err != nil {
		return fmt.Errorf("[table %s] %s", table, err.Error())
	}
	return nil
}

func (schemas *Schemas) Validate(dbname, table string, row *map[string]interface{}) error {
	databaseSchema, ok := (*schemas)[dbname]
	if !ok {
		return fmt.Errorf("did not find schema for database %s", dbname)
	}
	err := databaseSchema.Validate(table, row)
	if err != nil {
		return fmt.Errorf("[database %s] %s", dbname, err.Error())
	}
	return nil
}
