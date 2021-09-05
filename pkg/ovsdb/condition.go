package ovsdb

import (
	"errors"
	"fmt"

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

type Conditions []Condition

func NewCondition(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, condition []interface{}, log logr.Logger) (*Condition, error) {
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
			log.Error(err, "failed to unmarshal condition", "column", column, "type", columnSchema.Type, "value", value)
			return nil, err
		}
		value = tmp
	} else if column == libovsdb.COL_UUID { // TODO add libovsdb.COL_VERSION
		tmp, err := libovsdb.UnmarshalUUID(value)
		if err != nil {
			err = errors.New(E_INTERNAL_ERROR)
			log.Error(err, "failed to unmarshal condition", "column", column, "type", libovsdb.TypeUUID, "value", value)
			return nil, err
		}
		value = tmp
	}
	tmp, err := mapUUID.Resolve(value, log)
	if err != nil {
		err := errors.New(E_INTERNAL_ERROR)
		log.Error(err, "failed to resolve named-uuid condition", "column", column, "value", value)
		return nil, err
	}
	value = tmp

	cond := &Condition{
		Column:       column,
		Function:     fn,
		Value:        value,
		ColumnSchema: columnSchema,
		Log:          log,
	}
	err = cond.validateCompareFunction()
	if err != nil {
		return nil, err
	}
	return cond, nil
}

func (c Conditions) getUUIDIfSelected() (string, error) {
	for _, cond := range c {
		uuid, err := cond.getUUIDIfExists()
		if err != nil {
			return "", err
		}
		if uuid != "" {
			return uuid, nil
		}
	}
	return "", nil
}

func (c Conditions) isRowSelected(row *map[string]interface{}) (bool, error) {
	for _, cond := range c {
		ok, err := cond.Compare(row)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (c *Condition) validateCompareFunction() error {
	switch c.Function {
	case FN_EQ, FN_NE, FN_EX, FN_IN:
		// these functions are acceptable for all types
		return nil
	case FN_GE, FN_GT, FN_LE, FN_LT:
		if c.ColumnSchema.Type != libovsdb.TypeInteger && c.ColumnSchema.Type != libovsdb.TypeReal {
			err := errors.New(E_CONSTRAINT_VIOLATION)
			c.Log.Error(err, "incompatible compare function", "compare function", c.Function, "column type", c.ColumnSchema.Type)
			return err
		}
		return nil
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "unsupported compare function", "compare function", c.Function)
		return err
	}
}

func (c *Condition) CompareInteger(row *map[string]interface{}) (bool, error) {
	var err error
	// According to Decode docs, Unmarshal stores float64, for JSON numbers.
	// We will convert it to int before the comparison.
	val, ok := (*row)[c.Column].(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column])
		return false, err
	}
	if !(val == float64(int(val))) {
		c.Log.Error(err, "failed to convert row value", "value", (*row)[c.Column], "is not an integer")
		return false, err
	}

	actual := int(val)
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

	switch fn {
	case FN_EQ, FN_IN:
		return actual == expected, nil
	case FN_NE, FN_EX:
		return actual != expected, nil
	case FN_GT:
		return actual > expected, nil
	case FN_GE:
		return actual >= expected, nil
	case FN_LT:
		return actual < expected, nil
	case FN_LE:
		return actual <= expected, nil
	default:
		return false, nil
	}
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
	switch fn {
	case FN_EQ, FN_IN:
		return actual == expected, nil
	case FN_NE, FN_EX:
		return actual != expected, nil
	default:
		return false, nil
	}
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
	switch fn {
	case FN_EQ, FN_IN:
		return actual == expected, nil
	case FN_NE, FN_EX:
		return actual != expected, nil
	default:
		return false, nil
	}
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
	switch fn {
	case FN_EQ, FN_IN:
		return actual.GoUUID == expected.GoUUID, nil
	case FN_NE, FN_EX:
		return actual.GoUUID != expected.GoUUID, nil
	default:
		return false, nil
	}
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
	var actual libovsdb.OvsSet
	switch data := (*row)[c.Column].(type) {
	case libovsdb.OvsSet:
		actual = data
	case int, float64, bool, string, libovsdb.UUID, libovsdb.OvsMap:
		actual = libovsdb.OvsSet{GoSet: []interface{}{data}}
	default:
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

	switch fn {
	case FN_IN:
		return actual.IncludeSet(expected), nil
	case FN_EX:
		return actual.ExcludeSet(expected), nil
	case FN_EQ:
		return actual.Equals(expected), nil
	case FN_NE:
		return !actual.Equals(expected), nil
	default:
		return false, nil
	}
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

	switch fn {
	case FN_IN:
		return actual.IncludeMap(expected), nil
	case FN_EX:
		return actual.ExcludeMap(expected), nil
	case FN_EQ:
		return actual.Equals(expected), nil
	case FN_NE:
		return !actual.Equals(expected), nil
	default:
		return false, nil
	}
}

// a short cat for a most usual condition requests, when condition is uuid.
func (c *Condition) getUUIDIfExists() (string, error) {
	if c.Column != libovsdb.COL_UUID {
		return "", nil
	}
	if c.Function != FN_EQ && c.Function != FN_IN {
		return "", nil
	}
	ovsUUID, ok := c.Value.(libovsdb.UUID)
	if !ok {
		err := fmt.Errorf("failed to convert condition value %v, its type is %T", c.Value, c.Value)
		c.Log.Error(err, "")
		return "", err
	}
	return ovsUUID.GoUUID, nil
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
		c.Log.Error(err, "unsupported type comparison", "type", c.ColumnSchema.Type)
		return false, err
	}
}
