package libovsdb

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	// OperationInsert is an insert operation
	OperationInsert = "insert"
	// OperationSelect is a select operation
	OperationSelect = "select"
	// OperationUpdate is an update operation
	OperationUpdate = "update"
	// OperationMutate is a mutate operation
	OperationMutate = "mutate"
	// OperationDelete is a delete operation
	OperationDelete = "delete"
	// OperationWait is a wait operation
	OperationWait = "wait"
	// OperationCommit is a commit operation
	OperationCommit = "commit"
	// OperationAbort is an abort operation
	OperationAbort = "abort"
	// OperationComment is a comment operation
	OperationComment = "comment"
	// OperationAssert is an assert operation
	OperationAssert = "assert"
)

// Operation represents an operation according to RFC7047 section 5.2
type Operation struct {
	Op        string                    `json:"op"`
	Table     *string                   `json:"table,omitempty"`
	Row       *map[string]interface{}   `json:"row,omitempty"`
	Rows      *[]map[string]interface{} `json:"rows,omitempty"`
	Columns   *[]string                 `json:"columns,omitempty"`
	Mutations *[]interface{}            `json:"mutations,omitempty"`
	Timeout   *int                      `json:"timeout,omitempty"`
	Where     *[]interface{}            `json:"where,omitempty"`
	Until     *string                   `json:"until,omitempty"`
	UUIDName  *string                   `json:"uuid-name,omitempty"`
	UUID      *UUID                     `json:"uuid,omitempty"`
	Comment   *string                   `json:"comment,omitempty"`
	Durable   *bool                     `json:"durable,omitempty"`
}

// String, serialize Transact
func (o Operation) String() string {
	buf, err := json.Marshal(o)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal operation"))
	}
	return string(buf)
}

// MarshalJSON marshalls 'Operation' to a byte array
// For 'select' operations, we dont omit the 'Where' field
// to allow selecting all rows of a table
func (o Operation) MarshalJSON() ([]byte, error) {
	type OpAlias Operation
	switch o.Op {
	case "select":
		where := o.Where
		if where == nil {
			whereval := make([]interface{}, 0, 0)
			where = &whereval
		}
		return json.Marshal(&struct {
			Where *[]interface{} `json:"where,omitempty"`
			OpAlias
		}{
			Where:   where,
			OpAlias: (OpAlias)(o),
		})
	default:
		return json.Marshal(&struct {
			OpAlias
		}{
			OpAlias: (OpAlias)(o),
		})
	}
}

// MonitorRequests represents a group of monitor requests according to RFC7047
// We cannot use MonitorRequests by inlining the MonitorRequest Map structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type MonitorRequests struct {
	Requests map[string]MonitorRequest `json:"requests,overflow"`
}

// MonitorRequest represents a monitor request according to RFC7047
type MonitorRequest struct {
	Columns []string      `json:"columns,omitempty"`
	Select  MonitorSelect `json:"select,omitempty"`
}

// MonitorSelect represents a monitor select according to RFC7047
type MonitorSelect struct {
	Initial *bool `json:"initial,omitempty"`
	Insert  *bool `json:"insert,omitempty"`
	Delete  *bool `json:"delete,omitempty"`
	Modify  *bool `json:"modify,omitempty"`
}

func Bool(v bool) *bool { return &v }

func MSIsTrue(v *bool) bool { return v == nil || *v }

// TableUpdates is a collection of TableUpdate entries
// We cannot use TableUpdates directly by json encoding by inlining the TableUpdate Map
// structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type TableUpdates struct {
	Updates map[string]TableUpdate `json:"updates,overflow"`
}

// TableUpdate represents a table update according to RFC7047
type TableUpdate struct {
	Rows map[string]RowUpdate `json:"rows,overflow"`
}

// RowUpdate represents a row update according to RFC7047
type RowUpdate struct {
	New Row `json:"new,omitempty"`
	Old Row `json:"old,omitempty"`
}

// OvsdbError is an OVS Error Condition
type OvsdbError struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// NewCondition creates a new condition as specified in RFC7047
func NewCondition(column string, function string, value interface{}) []interface{} {
	return []interface{}{column, function, value}
}

// NewMutation creates a new mutation as specified in RFC7047
func NewMutation(column string, mutator string, value interface{}) []interface{} {
	return []interface{}{column, mutator, value}
}

// Transact represents the request of a Transact call
type Transact struct {
	DBName     string      `json:"dbname"`
	Operations []Operation `json:"operations"`
}

// String, serialize Transact
func (t *Transact) String() string {
	buf, err := json.Marshal(t)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal transaction"))
	}
	return string(buf)
}

func NewTransact(params []interface{}) (*Transact, error) {
	if len(params) < 2 {
		return nil, errors.New("malformed transaction")
	}

	tx := new(Transact)
	for i, v := range params {
		switch i {
		case 0:
			dbname, ok := v.(string)
			if !ok {
				return nil, errors.New("malformed transaction")
			}
			tx.DBName = dbname
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return nil, errors.New("malformed transaction")
			}
			var op Operation
			err = json.Unmarshal(b, &op)
			if err != nil {
				return nil, errors.New("malformed transaction")
			}
			tx.Operations = append(tx.Operations, op)
		}
	}

	return tx, nil
}

// TransactResponse represents the response to a Transact Operation
type TransactResponse struct {
	Result []OperationResult `json:"result,omitempty"`
	Error  *string           `json:"error,omitempty"`
}

// OperationResult is the result of an Operation
type OperationResult struct {
	Count   *int         `json:"count,omitempty"`
	Error   *string      `json:"error,omitempty"`
	Details *string      `json:"details,omitempty"`
	UUID    *UUID        `json:"uuid,omitempty"`
	Rows    *[]ResultRow `json:"rows,omitempty"`
}

func (res *OperationResult) SetError(err string) {
	res.Error = &err
	res.Count = nil
	res.UUID = nil
	res.Rows = nil
}

// String, serialize TransactResponse
func (res TransactResponse) String() string {
	buf, err := json.Marshal(res)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal operationa result, err: %s", err.Error()))
	}
	return string(buf)
}

func (res *OperationResult) InitUUID(uuid string) {
	res.UUID = &UUID{GoUUID: uuid}
}

func (res *OperationResult) InitRows() {
	res.Rows = new([]ResultRow)
}

func (res *OperationResult) InitCount() {
	res.Count = new(int)
}

func (res *OperationResult) IncrementCount() {
	*res.Count++
}

func (res *OperationResult) AppendRows(row ResultRow) {
	*res.Rows = append(*res.Rows, row)
}

func ovsSliceToGoNotation(val interface{}) (interface{}, error) {
	switch val.(type) {
	case []interface{}:
		sl := val.([]interface{})
		bsliced, err := json.Marshal(sl)
		if err != nil {
			return nil, err
		}

		switch sl[0] {
		case "uuid", "named-uuid":
			var uuid UUID
			err = json.Unmarshal(bsliced, &uuid)
			return uuid, err
		case "set":
			var oSet OvsSet
			err = json.Unmarshal(bsliced, &oSet)
			return oSet, err
		case "map":
			var oMap OvsMap
			err = json.Unmarshal(bsliced, &oMap)
			return oMap, err
		}
		return val, nil
	}
	return val, nil
}

// TODO : add Condition, Function, Mutation and Mutator notations
