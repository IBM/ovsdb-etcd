package libovsdb

import (
	"encoding/json"
	"fmt"
)

// Row is a table Row according to RFC7047
type Row struct {
	Fields map[string]interface{}
}

// UnmarshalJSON unmarshalls a byte array to an OVSDB Row
func (r *Row) UnmarshalJSON(b []byte) (err error) {
	r.Fields = make(map[string]interface{})
	var raw map[string]interface{}
	err = json.Unmarshal(b, &raw)
	for key, val := range raw {
		val, err = ovsSliceToGoNotation(val)
		if err != nil {
			return err
		}
		r.Fields[key] = val
	}
	return err
}

func (r *Row) GetUUID() (*UUID, error) {
	uuidInt, ok := r.Fields[COL_UUID]
	if !ok {
		return nil, fmt.Errorf("row doesn't contain %s", COL_UUID)
	}
	uuid, ok := uuidInt.(UUID)
	if !ok {
		return nil, fmt.Errorf("wrong uuid type %T %v", uuidInt, uuidInt)
	}
	return &uuid, nil
}

func (r *Row) deleteUUID() {
	delete(r.Fields, COL_UUID)
}

// ResultRow is an properly unmarshalled row returned by Transact
type ResultRow map[string]interface{}

// UnmarshalJSON unmarshalls a byte array to an OVSDB Row
func (r *ResultRow) UnmarshalJSON(b []byte) (err error) {
	*r = make(map[string]interface{})
	var raw map[string]interface{}
	err = json.Unmarshal(b, &raw)
	for key, val := range raw {
		val, err = ovsSliceToGoNotation(val)
		if err != nil {
			return err
		}
		(*r)[key] = val
	}
	return err
}
