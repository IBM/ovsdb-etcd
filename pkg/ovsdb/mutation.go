package ovsdb

import (
	"errors"
	"reflect"

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
	column       string
	mutator      string
	value        interface{}
	columnSchema *libovsdb.ColumnSchema
	log          logr.Logger
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
	if columnSchema.Type == libovsdb.TypeSet && isSetScalarOperation(columnSchema.TypeObj.Key.Type, mt) {
		value, err = columnSchema.TypeObj.Key.Unmarshal(value)
	} else {
		value, err = columnSchema.Unmarshal(value)
		if err != nil {
			// value for mutate map with delete mutator can be map or set
			if columnSchema.Type == libovsdb.TypeMap || mt == MT_DELETE {
				value, err = columnSchema.UnmarshalSet(value)
			}
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
		log.Error(err, "failed resolve namedUUID of column", "column", column)
		return nil, err
	}

	/*err = columnSchema.Validate(value)
	if err != nil {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		log.Error(err, "failed validate of column", "column", column)
		return nil, err
	}*/

	m := &Mutation{
		column:       column,
		mutator:      mt,
		value:        value,
		columnSchema: columnSchema,
		log:          log,
	}
	if err = m.validateMutator(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Mutation) validateMutator() error {
	switch m.columnSchema.Type {
	case libovsdb.TypeReal, libovsdb.TypeInteger:
		switch m.mutator {
		case MT_DIFFERENCE, MT_PRODUCT, MT_QUOTIENT, MT_SUM:
			return nil
		case MT_REMAINDER:
			if m.columnSchema.Type == libovsdb.TypeInteger {
				return nil
			}
		}
	case libovsdb.TypeMap:
		if m.mutator == MT_INSERT || m.mutator == MT_DELETE {
			return nil
		}
	case libovsdb.TypeSet:
		if m.mutator == MT_INSERT || m.mutator == MT_DELETE {
			return nil
		}
		if isSetScalarOperation(m.columnSchema.TypeObj.Key.Type, m.mutator) {
			return nil
		}
	}
	err := errors.New(E_CONSTRAINT_VIOLATION)
	m.log.Error(err, "incompatible mutator", "mutator", m.mutator, "column type", m.columnSchema.Type)
	return err
}

func (m *Mutation) mutateInteger(row *map[string]interface{}) error {
	var err error
	original := (*row)[m.column].(int)
	value, ok := m.value.(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "can't convert mutation value", "value", m.value)
		return err
	}
	mutated, err := m.mutateI(original, value)
	if err != nil {
		return err
	}
	(*row)[m.column] = mutated
	return nil
}

func (m *Mutation) mutateReal(row *map[string]interface{}) error {
	var err error
	original := (*row)[m.column].(float64)
	value, ok := m.value.(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "failed to convert mutation value", "value", m.value)
		return err
	}
	mutated, err := m.mutateR(original, value)
	if err != nil {
		return err
	}
	(*row)[m.column] = mutated
	return nil
}

func inSet(set *libovsdb.OvsSet, a interface{}) bool {
	for _, b := range set.GoSet {
		if reflect.DeepEqual(a, b) {
			return true
		}
	}
	return false
}

func (m *Mutation) insertToSet(original *libovsdb.OvsSet) (*libovsdb.OvsSet, error) {
	var err error
	toInsertSet, ok := m.value.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "failed to convert mutation value", "value", m.value)
		return nil, err
	}
	mutated := new(libovsdb.OvsSet)
	err = copier.Copy(mutated, original)
	if err != nil {
		m.log.Error(err, "copier failed")
		err = errors.New(E_CONSTRAINT_VIOLATION)
		return nil, err
	}
	for _, v := range toInsertSet.GoSet {
		if !inSet(original, v) {
			mutated.GoSet = append(mutated.GoSet, v)
		}
	}
	return mutated, nil
}

func (m *Mutation) mutateIntegersSet(original *libovsdb.OvsSet) (*libovsdb.OvsSet, error) {
	var err error
	value, ok := m.value.(int)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "can't convert mutation value", "value", m.value)
		return nil, err
	}

	mutated := new(libovsdb.OvsSet)
	for _, v := range original.GoSet {
		intV, ok := v.(int)
		if !ok {
			err = errors.New(E_CONSTRAINT_VIOLATION)
			m.log.Error(err, "can't convert mutated to int", "mutated", v)
			return nil, err
		}
		newV, err := m.mutateI(intV, value)
		if err != nil {
			return nil, err
		}
		mutated.GoSet = append(mutated.GoSet, newV)
	}
	return mutated, nil
}

func (m *Mutation) mutateRealsSet(original *libovsdb.OvsSet) (*libovsdb.OvsSet, error) {
	var err error
	value, ok := m.value.(float64)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "can't convert mutation value", "value", m.value)
		return nil, err
	}

	mutated := new(libovsdb.OvsSet)
	for _, v := range original.GoSet {
		r, ok := v.(float64)
		if !ok {
			err = errors.New(E_CONSTRAINT_VIOLATION)
			m.log.Error(err, "can't convert mutated to float64", "mutated", v)
			return nil, err
		}
		newV, err := m.mutateR(r, value)
		if err != nil {
			return nil, err
		}
		mutated.GoSet = append(mutated.GoSet, newV)
	}
	return mutated, nil
}

func (m *Mutation) deleteFromSet(original *libovsdb.OvsSet) (*libovsdb.OvsSet, error) {
	var err error
	toDeleteSet, ok := m.value.(libovsdb.OvsSet)
	if !ok {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "failed to convert mutation value", "value", m.value)
		return nil, err
	}
	mutated := new(libovsdb.OvsSet)
	for _, current := range original.GoSet {
		found := false
		for _, v := range toDeleteSet.GoSet {
			if reflect.DeepEqual(current, v) {
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

func (m *Mutation) mutateSet(row *map[string]interface{}) error {
	original := (*row)[m.column].(libovsdb.OvsSet)
	var mutated *libovsdb.OvsSet
	var err error
	switch m.mutator {
	case MT_INSERT:
		mutated, err = m.insertToSet(&original)
	case MT_DELETE:
		mutated, err = m.deleteFromSet(&original)
	case MT_PRODUCT, MT_REMAINDER, MT_SUM, MT_QUOTIENT, MT_DIFFERENCE:
		valueType := m.columnSchema.TypeObj.Key.Type
		if valueType == libovsdb.TypeInteger {
			mutated, err = m.mutateIntegersSet(&original)
		} else if valueType == libovsdb.TypeReal {
			mutated, err = m.mutateRealsSet(&original)
		} else {
			err = errors.New(E_CONSTRAINT_VIOLATION)
			m.log.Error(err, "incompatible mutator and set's value type:", "mutator", m.mutator, "value type", valueType)
		}
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "unsupported mutation mutator:", "mutator", m.mutator)
	}
	if err != nil {
		return err
	}
	(*row)[m.column] = *mutated
	return nil
}

func (m *Mutation) insertToMap(original *libovsdb.OvsMap, toInsert interface{}) (*libovsdb.OvsMap, error) {
	mutated := new(libovsdb.OvsMap)
	err := copier.Copy(&mutated, &original)
	if err != nil {
		m.log.Error(err, "copier failed")
		err = errors.New(E_CONSTRAINT_VIOLATION)
		return nil, err
	}
	switch toInsert := toInsert.(type) {
	case libovsdb.OvsMap:
		for k, v := range toInsert.GoMap {
			if _, ok := mutated.GoMap[k]; !ok {
				mutated.GoMap[k] = v
			}
		}
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "unsupported mutator value type", "value", toInsert)
		return nil, err
	}
	return mutated, nil
}

func (m *Mutation) deleteFromMap(original *libovsdb.OvsMap, toDelete interface{}) (*libovsdb.OvsMap, error) {
	mutated := new(libovsdb.OvsMap)
	err := copier.Copy(&mutated, &original)
	if err != nil {
		m.log.Error(err, "copier failed")
		err = errors.New(E_CONSTRAINT_VIOLATION)
		return nil, err
	}
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

func (m *Mutation) mutateMap(row *map[string]interface{}) error {
	original := (*row)[m.column].(libovsdb.OvsMap)
	mutated := new(libovsdb.OvsMap)
	var err error
	switch m.mutator {
	case MT_INSERT:
		mutated, err = m.insertToMap(&original, m.value)
	case MT_DELETE:
		mutated, err = m.deleteFromMap(&original, m.value)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "unsupported mutation mutator", "mutator", m.mutator)
		return err
	}
	if err != nil {
		return err
	}
	(*row)[m.column] = *mutated
	return nil
}

func (m *Mutation) Mutate(row *map[string]interface{}) error {
	var err error
	switch m.column {
	case libovsdb.COL_UUID, libovsdb.COL_VERSION:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "can't mutate column", "column", m.column)
		return err
	}
	if m.columnSchema.Mutable != nil && !*m.columnSchema.Mutable {
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "can't mutate immutable column", "column", m.column)
		return err
	}
	switch m.columnSchema.Type {
	case libovsdb.TypeInteger:
		return m.mutateInteger(row)
	case libovsdb.TypeReal:
		return m.mutateReal(row)
	case libovsdb.TypeSet:
		return m.mutateSet(row)
	case libovsdb.TypeMap:
		return m.mutateMap(row)
	default:
		err = errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "unsupported column schema type", "type", m.columnSchema.Type)
		return err
	}
}

func (m *Mutation) mutateI(mutated, value int) (int, error) {
	switch m.mutator {
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
			err := errors.New(E_DOMAIN_ERROR)
			m.log.Error(err, "can't divide by 0")
			return -1, err
		}
	case MT_REMAINDER:
		if value != 0 {
			mutated %= value
		} else {
			err := errors.New(E_DOMAIN_ERROR)
			m.log.Error(err, "can't modulo by 0")
			return -1, err
		}
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "unsupported mutator", "mutator", m.mutator)
		return -1, err
	}
	return mutated, nil
}

func (m *Mutation) mutateR(mutated, value float64) (float64, error) {
	switch m.mutator {
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
			err := errors.New(E_DOMAIN_ERROR)
			m.log.Error(err, "can't divide by 0")
			return -1, err
		}
	default:
		err := errors.New(E_CONSTRAINT_VIOLATION)
		m.log.Error(err, "unsupported mutator", "mutator", m.mutator)
		return -1, err
	}
	return mutated, nil
}

func isSetScalarOperation(setValuesType, mutator string) bool {
	switch setValuesType {
	case libovsdb.TypeInteger:
		switch mutator {
		case MT_DIFFERENCE, MT_PRODUCT, MT_QUOTIENT, MT_SUM, MT_REMAINDER:
			return true
		default:
			return false
		}
	case libovsdb.TypeReal:
		switch mutator {
		case MT_DIFFERENCE, MT_PRODUCT, MT_QUOTIENT, MT_SUM:
			return true
		default:
			return false
		}
	default:
		return false
	}
}
