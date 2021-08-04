package ovsdb

import (
	"errors"

	"github.com/go-logr/logr"
	"github.com/jinzhu/copier"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

const (
	MT_SUM        = "+="
	MT_DIFFERENCE = "-="
	MT_PRODUCT    = "*="
	MT_QUOTIENT   = "/="
	MT_REMAINDER  = "%="
	MT_INSERT     = "insert"
	MT_DELETE     = "delete"
)

type Mutation struct {
	Column       string
	Mutator      string
	Value        interface{}
	ColumnSchema *libovsdb.ColumnSchema
	Log          logr.Logger
}

func NewMutation(tableSchema *libovsdb.TableSchema, mapUUID namedUUIDResolver, mutation []interface{}, log logr.Logger) (*Mutation, error) {
	var err error
	if len(mutation) != 3 {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "expected 3 items in mutation object", "mutation", mutation)
		return nil, err
	}

	column, ok := mutation[0].(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "can't convert mutation column", mutation[0])
		return nil, err
	}

	columnSchema, err := tableSchema.LookupColumn(column)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "can't find column schema", "column", column)
		return nil, err
	}

	mt, ok := mutation[1].(string)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "can't convert mutation mutator", "mutator", mutation[1])
		return nil, err
	}

	value := mutation[2]
	value, err = columnSchema.Unmarshal(value)
	if err != nil {
		// value for mutate map with delete mutator can be map or set
		if columnSchema.Type == libovsdb.TypeMap || mt == MT_DELETE {
			value, err = columnSchema.UnmarshalSet(value)
		}
	}
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "failed unmarshal of column", "column", column)
		return nil, err
	}

	value, err = mapUUID.Resolve(value, log)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "failed resolv-namedUUID of column", "column", column)
		return nil, err
	}

	err = columnSchema.Validate(value)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "failed validate of column", "column", column)
		return nil, err
	}

	m := &Mutation{
		Column:       column,
		Mutator:      mt,
		Value:        value,
		ColumnSchema: columnSchema,
		Log:          log,
	}
	if err = m.validateMutator(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Mutation) validateMutator() error {
	switch m.ColumnSchema.Type {
	case libovsdb.TypeReal, libovsdb.TypeInteger:
		switch m.Mutator {
		case MT_DIFFERENCE, MT_PRODUCT, MT_QUOTIENT, MT_SUM:
			return nil
		case MT_REMAINDER:
			if m.ColumnSchema.Type == libovsdb.TypeInteger {
				return nil
			}
		}
	case libovsdb.TypeMap, libovsdb.TypeSet:
		if m.Mutator == MT_INSERT || m.Mutator == MT_DELETE {
			return nil
		}
		if m.ColumnSchema.Type == libovsdb.TypeSet {
			valueType := m.ColumnSchema.TypeObj.Value.Type
			if valueType == libovsdb.TypeReal || valueType == libovsdb.TypeInteger {
				switch m.Mutator {
				case MT_DIFFERENCE, MT_PRODUCT, MT_QUOTIENT, MT_SUM:
					return nil
				case MT_REMAINDER:
					if m.ColumnSchema.Type == libovsdb.TypeInteger {
						return nil
					}
				}
			}
			err := errors.New(E_CONSTRAINT_VIOLATION)
			m.Log.Error(err, "incompatible mutator", "mutator", m.Mutator, "set value type", valueType)
			return err
		}
	}
	err := errors.New(E_CONSTRAINT_VIOLATION)
	m.Log.Error(err, "incompatible mutator", "mutator", m.Mutator, "column type", m.ColumnSchema.Type)
	return err
}

func (m *Mutation) MutateInteger(row *map[string]interface{}) error {
	var err error
	original := (*row)[m.Column].(int)
	value, ok := m.Value.(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "can't convert mutation value", "value", m.Value)
		return err
	}
	mutated := original
	switch m.Mutator {
	case MT_SUM:
		mutated += value
	case MT_DIFFERENCE:
		mutated -= value
	case MT_PRODUCT:
		mutated *= value
	case MT_QUOTIENT:
		if value != 0 {
			mutated /= value
		} else {
			err = errors.New(E_DOMAIN_ERROR)
			m.Log.Error(err, "can't devide by 0")
			return err
		}
	case MT_REMAINDER:
		if value != 0 {
			mutated %= value
		} else {
			err = errors.New(E_DOMAIN_ERROR)
			m.Log.Error(err, "can't modulo by 0")
			return err
		}
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "unsupported mutator", "mutator", m.Mutator)
		return err
	}
	(*row)[m.Column] = mutated
	return nil
}

func (m *Mutation) MutateReal(row *map[string]interface{}) error {
	var err error
	original := (*row)[m.Column].(float64)
	value, ok := m.Value.(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "failed to convert mutation value", "value", m.Value)
		return err
	}
	mutated := original
	switch m.Mutator {
	case MT_SUM:
		mutated += value
	case MT_DIFFERENCE:
		mutated -= value
	case MT_PRODUCT:
		mutated *= value
	case MT_QUOTIENT:
		if value != 0 {
			mutated /= value
		} else {
			err = errors.New(E_DOMAIN_ERROR)
			m.Log.Error(err, "can't devide by 0")
			return err
		}
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "unsupported mutator", "mutator", m.Mutator)
		return err
	}
	(*row)[m.Column] = mutated
	return nil
}

func inSet(set *libovsdb.OvsSet, a interface{}) bool {
	for _, b := range set.GoSet {
		if isEqualValue(a, b) {
			return true
		}
	}
	return false
}

func (m *Mutation) insertToSet(original *libovsdb.OvsSet, toInsert interface{}) (*libovsdb.OvsSet, error) {
	var err error
	toInsertSet, ok := toInsert.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "failed to convert mutation value", "value", toInsert)
		return nil, err
	}
	mutated := new(libovsdb.OvsSet)
	copier.Copy(mutated, original)
	for _, v := range toInsertSet.GoSet {
		if !inSet(original, v) {
			mutated.GoSet = append(mutated.GoSet, v)
		}
	}
	return mutated, nil
}

func (m *Mutation) deleteFromSet(original *libovsdb.OvsSet, toDelete interface{}) (*libovsdb.OvsSet, error) {
	var err error
	toDeleteSet, ok := toDelete.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "failed to convert mutation value", "value", toDelete)
		return nil, err
	}
	mutated := new(libovsdb.OvsSet)
	for _, current := range original.GoSet {
		found := false
		for _, v := range toDeleteSet.GoSet {
			if isEqualValue(current, v) {
				found = true
				break
			}
		}
		if !found {
			mutated.GoSet = append(mutated.GoSet, current)
		}
	}
	return mutated, nil
}

func (m *Mutation) MutateSet(row *map[string]interface{}) error {
	original := (*row)[m.Column].(libovsdb.OvsSet)
	var mutated *libovsdb.OvsSet
	var err error
	switch m.Mutator {
	case MT_INSERT:
		mutated, err = m.insertToSet(&original, m.Value)
	case MT_DELETE:
		mutated, err = m.deleteFromSet(&original, m.Value)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "unsupported mutation mutator:", "mutator", m.Mutator)
	}
	if err != nil {
		return err
	}
	(*row)[m.Column] = *mutated
	return nil
}

func (m *Mutation) insertToMap(original *libovsdb.OvsMap, toInsert interface{}) (*libovsdb.OvsMap, error) {
	mutated := new(libovsdb.OvsMap)
	copier.Copy(&mutated, &original)
	switch toInsert := toInsert.(type) {
	case libovsdb.OvsMap:
		for k, v := range toInsert.GoMap {
			if _, ok := mutated.GoMap[k]; !ok {
				mutated.GoMap[k] = v
			}
		}
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "unsupported mutator value type", "value", toInsert)
		return nil, err
	}
	return mutated, nil
}

func (m *Mutation) deleteFromMap(original *libovsdb.OvsMap, toDelete interface{}) (*libovsdb.OvsMap, error) {
	mutated := new(libovsdb.OvsMap)
	copier.Copy(&mutated, &original)
	switch toDelete := toDelete.(type) {
	case libovsdb.OvsMap:
		for k, v := range toDelete.GoMap {
			if mutated.GoMap[k] == v {
				delete(mutated.GoMap, k)
			}
		}
	case libovsdb.OvsSet:
		for _, k := range toDelete.GoSet {
			delete(mutated.GoMap, k)
		}
	}
	return mutated, nil
}

func (m *Mutation) MutateMap(row *map[string]interface{}) error {
	original := (*row)[m.Column].(libovsdb.OvsMap)
	mutated := new(libovsdb.OvsMap)
	var err error
	switch m.Mutator {
	case MT_INSERT:
		mutated, err = m.insertToMap(&original, m.Value)
	case MT_DELETE:
		mutated, err = m.deleteFromMap(&original, m.Value)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "unsupported mutation mutator", "mutator", m.Mutator)
		return err
	}
	if err != nil {
		return err
	}
	(*row)[m.Column] = *mutated
	return nil
}

func (m *Mutation) Mutate(row *map[string]interface{}) error {
	var err error
	switch m.Column {
	case libovsdb.COL_UUID, libovsdb.COL_VERSION:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "can't mutate column", "column", m.Column)
		return err
	}
	if m.ColumnSchema.Mutable != nil && !*m.ColumnSchema.Mutable {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "can't mutate unmutable column", "column", m.Column)
		return err
	}
	switch m.ColumnSchema.Type {
	case libovsdb.TypeInteger:
		return m.MutateInteger(row)
	case libovsdb.TypeReal:
		return m.MutateReal(row)
	case libovsdb.TypeSet:
		return m.MutateSet(row)
	case libovsdb.TypeMap:
		return m.MutateMap(row)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.Log.Error(err, "unsupported column schema type", "type", m.ColumnSchema.Type)
		return err
	}
}
