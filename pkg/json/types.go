package json

import (
	"github.com/ebay/libovsdb"
)

// We probably move most of the types from this file to libovsdb progect
const ZERO_UUID = "00000000-0000-0000-0000-000000000000"

type Uuid string

type NamedUuid string

type Map map[string]string

type Set []interface{}

type EmptyStruct struct{}

// maps from a table name to a tableUpdate
type TableUpdates map[string]TableUpdate

// maps from row’s UUID to a RowUpdate> object
type TableUpdate map[string]RowUpdate

// RowUpdate represents a row update according to RFC7047 and
// https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/ extensions.
type RowUpdate struct {
	New     *map[string]interface{} `json:"new,omitempty"`
	Old     *map[string]interface{} `json:"old,omitempty"`
	Initial *map[string]interface{} `json:"initial,omitempty"`
	Insert  *map[string]interface{} `json:"insert,omitempty"`
	Delete  *map[string]interface{} `json:"delete,omitempty"`
	Modify  *map[string]interface{} `json:"modify,omitempty"`
}

// RowUpdate can contains or the `New` and `Old` values according to RFC7047, or one of the `Initial`, `Insert`, `Delete`
// or `Modify` objects.
// Id the RowUpdate object is not valid, the method returns <false> and an explanation message
func (ru *RowUpdate) Validate() (bool, string) {
	i := 0
	if ru.Initial != nil {
		i++
	}
	if ru.Insert != nil {
		i++
	}
	if ru.Delete != nil {
		i++
	}
	if ru.Modify != nil {
		i++
	}
	if i > 1 {
		return false, "Multiple RowUpdate2 entries"
	} else if i == 1 {
		if ru.New != nil || ru.Old != nil {
			return false, "Combination of RowUpdate and RowUpdate2"
		}
		return true, ""
	}
	// i ==0
	if (ru.New == nil) && (ru.Old == nil) {
		return false, "Empty RowUpdate"
	}
	return true, ""
}

type UpdateNotification struct {
	JasonValue   string
	TableUpdates map[string]TableUpdate
	Uuid         *libovsdb.UUID
}

type UpdateNotificationType int

const (
	Update UpdateNotificationType = iota
	Update2
	Update3
)

type MonitorCondRequest struct {
	Columns []interface{}           `json:"columns,omitempty"`
	Where   [][]string              `json:"where,omitempty"`
	Select  *libovsdb.MonitorSelect `json:"select,omitempty"`
}

type CondMonitorParameters struct {
	DatabaseName string
	JsonValue    []string // TODO
	// maps table name to MonitorCondRequests
	MonitorCondRequests map[string][]MonitorCondRequest
	LastTxnID           string
	UpdateType          UpdateNotificationType
}

// TODO to be updated
type TransactionResponse struct {
	Rows []map[string]string
}

func (uuid Uuid) String() string {
	return string(uuid)
}
