package ovsjson

import (
	"encoding/json"
	"fmt"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
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
	New     *map[string]interface{}
	Old     *map[string]interface{}
	Initial *map[string]interface{}
	Insert  *map[string]interface{}
	Delete  bool
	Modify  *map[string]interface{}
}

// String, serialize Operation TableUpdate
func (tu TableUpdate) String() string {
	buf, err := json.Marshal(tu)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal operationa result, err: %s", err.Error()))
	}
	return string(buf)
}

// String, serialize Operation RowUpdate
func (ru RowUpdate) String() string {
	buf, err := json.Marshal(ru)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal operationa result, err: %s", err.Error()))
	}
	return string(buf)
}

// Validate that the RowUpdate is valid as RowUpdate according to the RFC 7047
// Only `New` and `Old` fields can contain data
// If the RowUpdate object is not valid, the method returns <false> and an explanation message
func (ru *RowUpdate) ValidateRowUpdate() (bool, string) {
	i := 0
	if ru.Initial != nil {
		i++
	}
	if ru.Insert != nil {
		i++
	}
	if ru.Delete {
		i++
	}
	if ru.Modify != nil {
		i++
	}
	if i != 0 {
		return false, "Contains RowUpdate2 entries"
	}
	if (ru.New == nil) && (ru.Old == nil) {
		return false, "Empty RowUpdate"
	}
	return true, ""
}

// Validate that the RowUpdate is valid as RowUpdate2 according to https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/
// Only one of the `Initial`, `Insert`, `Delete` or `Modify` fields can contain data
// If the RowUpdate object is not valid, the method returns <false> and an explanation message
func (ru *RowUpdate) ValidateRowUpdate2() (bool, string) {
	i := 0
	if ru.Initial != nil {
		i++
	}
	if ru.Insert != nil {
		i++
	}
	if ru.Delete {
		i++
	}
	if ru.Modify != nil {
		i++
	}
	if i > 1 {
		return false, "Contains several RowUpdate2 entries"
	}
	if (ru.New != nil) || (ru.Old != nil) {
		return false, "Contains RowUpdate entries"
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
	Columns []string                `json:"columns,omitempty"`
	Where   interface{}             `json:"where,omitempty"` // TODO fix type (should be []string, or [][]string, but sometimes it is boolean
	Select  *libovsdb.MonitorSelect `json:"select,omitempty"`
}

type CondMonitorParameters struct {
	DatabaseName        string                          `json:"db-name,omitempty"`
	JsonValue           string                          `json:"json-value"`    // can be nil
	MonitorCondRequests map[string][]MonitorCondRequest `json:"mcr,omitempty"` // maps tableName to MonitorCondRequests
	LastTxnID           *string                         `json:"last-txn-id,omitempty"`
}

func (cmp CondMonitorParameters) String() string {
	buf, err := json.Marshal(cmp)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal operationa result, err: %s", err.Error()))
	}
	return string(buf)
}

func (uuid Uuid) String() string {
	return string(uuid)
}
