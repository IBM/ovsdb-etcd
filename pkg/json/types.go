package json

import (
	"github.com/ebay/libovsdb"
)

const ZERO_UUID = "00000000-0000-0000-0000-000000000000"

type Uuid string

type NamedUuid string

type Map map[string]string

type Set []interface{}

type EmptyStruct struct{}

// used as a base type for `Initial', 'Insert', 'Delete' and 'Modify'
type RowUpdate2 interface{}

// presents for initial updates
type Initial struct {
	RowUpdate2 `json:"-,omitempty"`
	Initial    interface{} `json:"initial,omitempty"`
}

// presents for insert updates
type Insert struct {
	RowUpdate2
	Insert interface{} `json:"insert,omitempty"`
}

// presents for delete updates
type Delete struct {
	RowUpdate2
	Delete interface{} `json:"delete,omitempty"`
}

// presents for modify updates
type Modify struct {
	RowUpdate2
	Modify interface{} `json:"modify,omitempty"`
}

type MonitorCondRequest struct {
	Columns []interface{}           `json:"columns,omitempty"`
	Where   [][]string              `json:"where,omitempty"`
	Select  *libovsdb.MonitorSelect `json:"select,omitempty"`
}

type CondMonitorParameters struct {
	DatabaseName        string
	JsonValue           []string // TODO
	MonitorCondRequests map[string][]MonitorCondRequest
	LastTxnID           string
}

// TODO to be updated
type TransactionResponse struct {
	Rows []map[string]string
}

func (uuid Uuid) String() string {
	return string(uuid)
}
