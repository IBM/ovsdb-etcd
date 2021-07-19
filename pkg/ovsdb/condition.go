package ovsdb

import (
	"errors"

	"github.com/go-logr/logr"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

const (
	FN_LT = "<"
	FN_LE = "<="
	FN_EQ = "=="
	FN_NE = "!="
	FN_GE = ">="
	FN_GT = ">"
	FN_IN = "includes"
	FN_EX = "excludes"
)

type Condition struct {
	Column       string
	Function     string
	Value        interface{}
	ColumnSchema *libovsdb.ColumnSchema
	Log          logr.Logger
}

func NewCondition(log logr.Logger, tableSchema *libovsdb.TableSchema, mapUUID MapUUID, condition []interface{}) (*Condition, error) {
	var err error
	if len(condition) != 3 {
		err = errors.New(E_INTERNAL_ERROR)
		log.Error(err, "expected 3 elements in condition", "condition", condition)
		return nil, err
	}

	column, ok := condition[0].(string)
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		log.Error(err, "failed to convert column to string", "condition", condition)
		return nil, err
	}

	columnSchema, err := tableSchema.LookupColumn(column)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "failed schema lookup", "column", column)
		return nil, err
	}

	fn, ok := condition[1].(string)
	if !ok {
		err = errors.New(E_INTERNAL_ERROR)
		log.Error(err, "failed to convert function to string", "condition", condition)
		return nil, err
	}

	value := condition[2]
	if columnSchema != nil {
		tmp, err := columnSchema.Unmarshal(value)
		if err != nil {
			err = errors.New(E_INTERNAL_ERROR)
			log.Error(err, "failed to unmarsahl condition", "column", column, "type", columnSchema.Type, "value", value)
			return nil, err
		}
		value = tmp
	} else if column == libovsdb.COL_UUID {
		tmp, err := libovsdb.UnmarshalUUID(value)
		if err != nil {
			err = errors.New(E_INTERNAL_ERROR)
			log.Error(err, "failed to unamrshal condition", "column", column, "type", libovsdb.TypeUUID, "value", value)
			return nil, err
		}
		value = tmp
	}

	tmp, err := mapUUID.Resolv(log, value)
	if err != nil {
		err := errors.New(E_INTERNAL_ERROR)
		log.Error(err, "failed to resolve named-uuid condition", "column", column, "value", value)
		return nil, err
	}
	value = tmp

	return &Condition{
		Column:       column,
		Function:     fn,
		Value:        value,
		ColumnSchema: columnSchema,
		Log:          log,
	}, nil
}

func (c *Condition) CompareInteger(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}
	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	if fn == FN_GT && actual > expected {
		return true, nil
	}
	if fn == FN_GE && actual >= expected {
		return true, nil
	}
	if fn == FN_LT && actual < expected {
		return true, nil
	}
	if fn == FN_LE && actual <= expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareReal(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	if fn == FN_GT && actual > expected {
		return true, nil
	}
	if fn == FN_GE && actual >= expected {
		return true, nil
	}
	if fn == FN_LT && actual < expected {
		return true, nil
	}
	if fn == FN_LE && actual <= expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareBoolean(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(bool)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(bool)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareString(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual == expected {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual != expected {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareUUID(row *map[string]interface{}) (bool, error) {
	var err error
	var actual libovsdb.UUID
	ar, ok := (*row)[c.Column].([]interface{})
	if ok {
		actual = libovsdb.UUID{GoUUID: ar[1].(string)}
	} else {
		actual, ok = (*row)[c.Column].(libovsdb.UUID)
		if !ok {
			err = errors.New(E_CONSTRAINT_VIOLATION)
			c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
			return false, err
		}
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.UUID)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && actual.GoUUID == expected.GoUUID {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && actual.GoUUID != expected.GoUUID {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareEnum(row *map[string]interface{}) (bool, error) {
	var err error
	switch c.ColumnSchema.TypeObj.Key.Type {
	case libovsdb.TypeString:
		return c.CompareString(row)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "does not support type as enum key", "type", c.ColumnSchema.TypeObj.Key.Type)
		return false, err
	}
}

func (c *Condition) CompareSet(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && isEqualSet(actual, expected) {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && !isEqualSet(actual, expected) {
		return true, nil
	}
	return false, nil
}

func (c *Condition) CompareMap(row *map[string]interface{}) (bool, error) {
	var err error
	actual, ok := (*row)[c.Column].(libovsdb.OvsMap)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	fn := c.Function
	expected, ok := c.Value.(libovsdb.OvsMap)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert condition value", "value", c.Value)
		return false, err
	}

	if (fn == FN_EQ || fn == FN_IN) && isEqualMap(actual, expected) {
		return true, nil
	}
	if (fn == FN_NE || fn == FN_EX) && !isEqualMap(actual, expected) {
		return true, nil
	}
	return false, nil
}

func (c *Condition) Compare(row *map[string]interface{}) (bool, error) {
	switch c.ColumnSchema.Type {
	case libovsdb.TypeInteger:
		return c.CompareInteger(row)
	case libovsdb.TypeReal:
		return c.CompareReal(row)
	case libovsdb.TypeBoolean:
		return c.CompareBoolean(row)
	case libovsdb.TypeString:
		return c.CompareString(row)
	case libovsdb.TypeUUID:
		return c.CompareUUID(row)
	case libovsdb.TypeEnum:
		return c.CompareEnum(row)
	case libovsdb.TypeSet:
		return c.CompareSet(row)
	case libovsdb.TypeMap:
		return c.CompareMap(row)
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "usupported type comparison", "type", c.ColumnSchema.Type)
		return false, err
	}
}
