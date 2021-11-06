package ovsdb

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

var log logr.Logger

func init() {
	fs := flag.NewFlagSet("fs", flag.PanicOnError)
	klog.InitFlags(fs)
	err := fs.Set("v", "10")
	if err != nil {
		klog.Errorf("flags set %v", err)
		os.Exit(1)
	}
	defer klog.Flush()
	log = klogr.New()
}

const (
	dbName    = "dbName"
	tableName = "T1"
	rowUUID   = "43f24179-432d-435b-a8dc-e7134cf39e32"
	lastTNX   = "00000000-0000-0000-0000-000000000000"
	setColumn = "set"
	mapColumn = "map"
	keyPrefix = "ovsdb/nb"
)

func TestMonitorPrepareRowCheckColumns(t *testing.T) {
	tableSchema := createTestTableSchema()
	data := map[string]interface{}{"c1": "v1", "c2": "v2"}
	expectedUUID := guuid.NewString()
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	row := &libovsdb.Row{Fields: data}

	// Columns are nil or all columns
	expRow := map[string]interface{}{"c1": "v1", "c2": "v2"}
	checkPrepareRow(t, tableSchema, row, false, ovsjson.MonitorCondRequest{}, expRow)
	checkPrepareRow(t, tableSchema, row, true, ovsjson.MonitorCondRequest{Columns: &[]string{"c1", "c2"}}, expRow)
	checkPrepareRow(t, tableSchema, row, true, ovsjson.MonitorCondRequest{Columns: &[]string{"c1", "c2"}}, expRow)

	// Columns are empty array or a different column
	expRow = map[string]interface{}{}
	checkPrepareRow(t, tableSchema, row, false, ovsjson.MonitorCondRequest{Columns: &[]string{""}}, expRow)
	checkPrepareRow(t, tableSchema, row, true, ovsjson.MonitorCondRequest{Columns: &[]string{"c3"}}, expRow)

	// Single Column
	expRow = map[string]interface{}{"c2": "v2"}
	checkPrepareRow(t, tableSchema, row, false, ovsjson.MonitorCondRequest{Columns: &[]string{"c2"}}, expRow)
}

func TestMonitorPrepareRowCheckWhere(t *testing.T) {
	const (
		setColumn0 = setColumn + "0"
		setColumn1 = setColumn + "1"
		setColumn2 = setColumn + "2"
		mapColumn0 = mapColumn + "0"
		mapColumn1 = mapColumn + "1"
	)
	tableSchema := createTestTableSchema()
	set0 := libovsdb.OvsSet{GoSet: nil}
	set1 := libovsdb.OvsSet{GoSet: []interface{}{"a"}}
	set2 := libovsdb.OvsSet{GoSet: []interface{}{"a", "b"}}
	map0 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}
	map1 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}
	dataRow := map[string]interface{}{"c1": "v1", "c2": "v2", "r1": 1.5, "i1": 3, "b1": true, setColumn0: set0, setColumn1: set1, setColumn2: set2, mapColumn0: map0, mapColumn1: map1}
	data := map[string]interface{}{"c1": "v1", "c2": "v2", "r1": 1.5, "i1": 3, "b1": true, setColumn0: set0, setColumn1: set1, setColumn2: set2, mapColumn0: map0, mapColumn1: map1}
	expectedUUID := rowUUID
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	// we need marshal and unmarshal to transfer integers to float64
	buf, err := json.Marshal(data)
	assert.Nil(t, err)
	row := libovsdb.Row{}
	err = row.UnmarshalJSON(buf)
	assert.Nil(t, err)

	emptyRow := map[string]interface{}{}
	checkWhere := func(Where *[]interface{}, expRow map[string]interface{}) {
		checkPrepareRow(t, tableSchema, &row, false, ovsjson.MonitorCondRequest{Where: Where}, expRow)
	}

	// booleans
	checkWhere(&[]interface{}{true}, dataRow)
	checkWhere(&[]interface{}{false}, emptyRow)
	checkWhere(&[]interface{}{true, true}, dataRow)
	checkWhere(&[]interface{}{true, false}, emptyRow)

	// Type strings - one statement
	checkWhere(&[]interface{}{[3]interface{}{"c1", "==", "v1"}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "==", "v1"}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "includes", "v1"}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "!=", "v2"}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "excludes", "v2"}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{"c1", "==", "v2"}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "includes", "v2"}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "!=", "v1"}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "excludes", "v1"}}, emptyRow)

	// Type strings - multiple statements
	checkWhere(&[]interface{}{[]interface{}{"c1", "==", "v1"}, []interface{}{"c2", "==", "v2"}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "==", "v1"}, true}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "==", "v1"}, []interface{}{"c2", "!=", "v3"}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "==", "v1"}, []interface{}{"c2", "!=", "v3"}, true}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"c1", "==", "v1"}, []interface{}{"c2", "!=", "v3"}, false}, emptyRow)

	// Type real
	checkWhere(&[]interface{}{[]interface{}{"r1", "==", 1.5}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "includes", 1.5}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "<=", 1.5}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", ">=", 1.5}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "!=", 0.5}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "excludes", 0.5}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{"r1", "==", 0.5}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "includes", 0.5}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "!=", 1.5}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "excludes", 1.5}}, emptyRow)

	checkWhere(&[]interface{}{[]interface{}{"r1", ">", 1.5}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "<", 1.5}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", ">", 0.5}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"r1", "<", 2.5}}, dataRow)

	// Type int
	checkWhere(&[]interface{}{[]interface{}{"i1", "==", 3}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "includes", 3}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "<=", 3}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", ">=", 3}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "!=", 1}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "excludes", 1}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{"i1", "==", 1}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "includes", 1}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "!=", 3}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "excludes", 3}}, emptyRow)

	checkWhere(&[]interface{}{[]interface{}{"i1", ">", 3}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "<", 3}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", ">", 1}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"i1", "<", 5}}, dataRow)

	// Type bool
	checkWhere(&[]interface{}{[]interface{}{"b1", "==", true}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"b1", "includes", true}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"b1", "!=", false}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{"b1", "excludes", false}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{"b1", "==", false}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"b1", "includes", false}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"b1", "!=", true}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{"b1", "excludes", true}}, emptyRow)

	// Type UUID
	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "==", libovsdb.UUID{GoUUID: expectedUUID}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "includes", libovsdb.UUID{GoUUID: expectedUUID}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "!=", libovsdb.UUID{GoUUID: lastTNX}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "excludes", libovsdb.UUID{GoUUID: lastTNX}}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "==", libovsdb.UUID{GoUUID: lastTNX}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "includes", libovsdb.UUID{GoUUID: lastTNX}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "!=", libovsdb.UUID{GoUUID: expectedUUID}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{libovsdb.ColUuid, "excludes", libovsdb.UUID{GoUUID: expectedUUID}}}, emptyRow)

	// Type Set with Zero elements
	checkWhere(&[]interface{}{[]interface{}{setColumn0, "==", libovsdb.OvsSet{GoSet: []interface{}{}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn0, "includes", libovsdb.OvsSet{GoSet: nil}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn0, "!=", libovsdb.OvsSet{GoSet: nil}}}, emptyRow)

	checkWhere(&[]interface{}{[]interface{}{setColumn0, "==", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn0, "includes", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn0, "!=", libovsdb.OvsSet{GoSet: nil}}}, emptyRow)

	// Type Set with one element
	checkWhere(&[]interface{}{[]interface{}{setColumn1, "==", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn1, "includes", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn1, "!=", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn1, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{setColumn1, "==", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn1, "includes", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn1, "!=", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn1, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, emptyRow)

	// Type Set with 2 elements
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "==", libovsdb.OvsSet{GoSet: []interface{}{"a", "b"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "==", libovsdb.OvsSet{GoSet: []interface{}{"b", "a"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "!=", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "includes", libovsdb.OvsSet{GoSet: []interface{}{"a", "b"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "includes", libovsdb.OvsSet{GoSet: []interface{}{"b", "a"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "includes", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "includes", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"c"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"c", "d"}}}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{setColumn2, "==", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "!=", libovsdb.OvsSet{GoSet: []interface{}{"a", "b"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "!=", libovsdb.OvsSet{GoSet: []interface{}{"b", "a"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"a", "b"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"b", "a"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"a"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "excludes", libovsdb.OvsSet{GoSet: []interface{}{"b"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "includes", libovsdb.OvsSet{GoSet: []interface{}{"c"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "includes", libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{setColumn2, "includes", libovsdb.OvsSet{GoSet: []interface{}{"c", "d"}}}}, emptyRow)

	// Type Map

	// Type Map without any elements
	checkWhere(&[]interface{}{[]interface{}{mapColumn0, "==", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn0, "includes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn0, "!=", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn0, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{mapColumn0, "!=", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn0, "==", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn0, "includes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, emptyRow)

	// Type Map with 2 tuples
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "==", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "!=", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "includes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "includes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "includes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key2": "val2"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key3": "val1"}}}}, dataRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val3"}}}}, dataRow)

	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "!=", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "==", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "includes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2", "key3": "val3"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1", "key2": "val2"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val1"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key2": "val2"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key3": "val1", "key2": "val2"}}}}, emptyRow)
	checkWhere(&[]interface{}{[]interface{}{mapColumn1, "excludes", libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "val3", "key2": "val2"}}}}, emptyRow)
}

func checkPrepareRow(t *testing.T, tableSchema *libovsdb.TableSchema, row *libovsdb.Row, isV1 bool, mcr ovsjson.MonitorCondRequest, expRow map[string]interface{}) {
	updater := *mcrToUpdater(mcr, "", tableSchema, isV1, log)
	data, _, err := updater.prepareRow(row)
	assert.Nil(t, err)
	isEqualMaps(t, &expRow, &data, tableSchema)
}

func createTestTableSchema() *libovsdb.TableSchema {
	var tableSchema libovsdb.TableSchema
	columnSchemaUUID := libovsdb.ColumnSchema{Type: libovsdb.TypeUUID}
	columnSchemaString := libovsdb.ColumnSchema{Type: libovsdb.TypeString}
	columnSchemaReal := libovsdb.ColumnSchema{Type: libovsdb.TypeReal}
	columnSchemaInt := libovsdb.ColumnSchema{Type: libovsdb.TypeInteger}
	columnSchemaBool := libovsdb.ColumnSchema{Type: libovsdb.TypeBoolean}
	setColumnType := libovsdb.ColumnType{Key: &libovsdb.BaseType{Type: "string"}}
	sColumnSchema := libovsdb.ColumnSchema{Type: libovsdb.TypeSet, TypeObj: &setColumnType}
	mapColumnType := libovsdb.ColumnType{Key: &libovsdb.BaseType{Type: "string"}, Value: &libovsdb.BaseType{Type: "string"}}
	mColumnSchema := libovsdb.ColumnSchema{Type: libovsdb.TypeMap, TypeObj: &mapColumnType}
	tableSchema.Columns = map[string]*libovsdb.ColumnSchema{
		"c1":             &columnSchemaString,
		"c2":             &columnSchemaString,
		"c3":             &columnSchemaString,
		"c4":             &columnSchemaString,
		"r1":             &columnSchemaReal,
		"i1":             &columnSchemaInt,
		"b1":             &columnSchemaBool,
		setColumn + "0":  &sColumnSchema,
		setColumn + "1":  &sColumnSchema,
		setColumn + "2":  &sColumnSchema,
		libovsdb.ColUuid: &columnSchemaUUID,
		mapColumn + "0":  &mColumnSchema,
		mapColumn + "1":  &mColumnSchema,
	}
	return &tableSchema
}

func TestMonitorPrepareInitialRow(t *testing.T) {
	tableSchema := createTestTableSchema()
	data := map[string]interface{}{"c1": "v1", "c2": "v2"}
	expectedUUID := guuid.NewString()
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	row := libovsdb.Row{Fields: data}
	testMonitorPrepareInitialRow_(t, tableSchema, row, expectedUUID, true)
	testMonitorPrepareInitialRow_(t, tableSchema, row, expectedUUID, false)
}

func testMonitorPrepareInitialRow_(t *testing.T, tableSchema *libovsdb.TableSchema, data libovsdb.Row, expectedUUID string, isV1 bool) {

	var expRow *ovsjson.RowUpdate

	// Columns are all columns
	if isV1 {
		expRow = &ovsjson.RowUpdate{New: &map[string]interface{}{"c1": "v1", "c2": "v2"}}
	} else {
		expRow = &ovsjson.RowUpdate{Initial: &map[string]interface{}{"c1": "v1", "c2": "v2"}}
	}
	updater := *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", tableSchema, isV1, log)
	row, uuid, err := updater.prepareInitialRow(data)
	assert.Nil(t, err)
	assert.Equal(t, expRow, row)
	assert.Equal(t, expectedUUID, uuid)
	updater = *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: &[]string{"c1", "c2"}}, "", tableSchema, isV1, log)
	row, uuid, err = updater.prepareInitialRow(data)
	assert.Nil(t, err)
	assert.Equal(t, expRow, row)

	// Columns are empty array
	if isV1 {
		expRow = &ovsjson.RowUpdate{New: &map[string]interface{}{}}
	} else {
		expRow = &ovsjson.RowUpdate{Initial: &map[string]interface{}{}}
	}
	updater = *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: &[]string{}}, "", tableSchema, isV1, log)
	row, uuid, err = updater.prepareInitialRow(data)
	assert.Nil(t, err)
	assert.Equal(t, expRow, row)
}

func TestMonitorPrepareInsertRow(t *testing.T) {
	tableSchema := createTestTableSchema()
	expectedUUID := guuid.NewString()
	data := map[string]interface{}{"c1": "v1", "c2": "v2"}
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	dataJson, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	event := &clientv3.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID),
		Value: dataJson, CreateRevision: 1, ModRevision: 1}}
	testMonitorPrepareInsertRow_(t, tableSchema, event, expectedUUID, true)
	testMonitorPrepareInsertRow_(t, tableSchema, event, expectedUUID, false)
}

func testMonitorPrepareInsertRow_(t *testing.T, tableSchema *libovsdb.TableSchema, event *clientv3.Event, expectedUUID string, isV1 bool) {

	var expRow *ovsjson.RowUpdate

	// Columns are all columns
	if isV1 {
		expRow = &ovsjson.RowUpdate{New: &map[string]interface{}{"c1": "v1", "c2": "v2"}}
	} else {
		expRow = &ovsjson.RowUpdate{Insert: &map[string]interface{}{"c1": "v1", "c2": "v2"}}
	}
	updater := mcrToUpdater(ovsjson.MonitorCondRequest{}, "", tableSchema, isV1, log)
	validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)

	// Columns are empty array
	if isV1 {
		expRow = &ovsjson.RowUpdate{New: &map[string]interface{}{}}
	} else {
		expRow = &ovsjson.RowUpdate{Insert: &map[string]interface{}{}}
	}
	updater = mcrToUpdater(ovsjson.MonitorCondRequest{Columns: &[]string{}}, "", tableSchema, isV1, log)
	validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)
}

func TestMonitorPrepareDeleteRow(t *testing.T) {
	tableSchema := createTestTableSchema()
	expectedUUID := guuid.NewString()
	data := map[string]interface{}{"c1": "v1", "c2": "v2"}
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	dataJson, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	event := &clientv3.Event{Type: mvccpb.DELETE,
		PrevKv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID), Value: dataJson},
		Kv:     &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID)}}

	testMonitorPrepareDeleteRow_(t, tableSchema, event, expectedUUID, true)
	testMonitorPrepareDeleteRow_(t, tableSchema, event, expectedUUID, false)
}

func testMonitorPrepareDeleteRow_(t *testing.T, tableSchema *libovsdb.TableSchema, event *clientv3.Event, expectedUUID string, isV1 bool) {
	var expRow *ovsjson.RowUpdate

	// Columns are all columns
	if isV1 {
		expRow = &ovsjson.RowUpdate{Old: &map[string]interface{}{"c1": "v1", "c2": "v2"}}
	} else {
		expRow = &ovsjson.RowUpdate{Delete: true}
	}
	updater := mcrToUpdater(ovsjson.MonitorCondRequest{}, "", tableSchema, isV1, log)
	validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)

	// Columns are empty array
	if isV1 {
		expRow = &ovsjson.RowUpdate{Old: &map[string]interface{}{}}
	} else {
		expRow = &ovsjson.RowUpdate{Delete: true}
	}
	updater = mcrToUpdater(ovsjson.MonitorCondRequest{Columns: &[]string{}}, "", tableSchema, isV1, log)
	validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)
}

func TestMonitorPrepareModifyRow(t *testing.T) {
	tableSchema := createTestTableSchema()
	expectedUUID := guuid.NewString()
	data := map[string]interface{}{"c1": "v1", "c2": "v2", "c3": "v3"}
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	data1Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	data["c2"] = "v3"
	delete(data, "c3")
	data["c4"] = "v4"
	data2Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	event := &clientv3.Event{Type: mvccpb.PUT,
		PrevKv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID), Value: data1Json},
		Kv:     &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID), Value: data2Json, CreateRevision: 1, ModRevision: 2}}

	testMonitorPrepareModifyRow_(t, tableSchema, event, expectedUUID, true)
	testMonitorPrepareModifyRow_(t, tableSchema, event, expectedUUID, false)
}

func testMonitorPrepareModifyRow_(t *testing.T, tableSchema *libovsdb.TableSchema, event *clientv3.Event, expectedUUID string, isV1 bool) {
	var expRow *ovsjson.RowUpdate

	// Columns are all columns
	if isV1 {
		expRow = &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2", "c3": "v3"},
			New: &map[string]interface{}{"c1": "v1", "c2": "v3", "c4": "v4"}}
	} else {
		expRow = &ovsjson.RowUpdate{Modify: &map[string]interface{}{"c2": "v3", "c3": "v3", "c4": "v4"}}
	}
	updater := mcrToUpdater(ovsjson.MonitorCondRequest{}, "", tableSchema, isV1, log)
	validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)

	// TODO validate
	// Columns are empty array
	expRow = nil
	updater = mcrToUpdater(ovsjson.MonitorCondRequest{Columns: &[]string{}}, "", tableSchema, isV1, log)
	validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)
}

func TestMonitorPrepareModifyMapRow(t *testing.T) {
	tableSchema := createTestTableSchema()
	expectedUUID := guuid.NewString()
	colMap := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"k1": "v1", "k2": "v2", "k3": "v3"}}
	mapColName := mapColumn + "0"
	data := map[string]interface{}{"c1": "v1", mapColName: colMap}
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	data1Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	colMap.GoMap["k2"] = "v3"
	delete(colMap.GoMap, "k3")
	colMap.GoMap["k4"] = "v4"
	data2Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	event := &clientv3.Event{Type: mvccpb.PUT,
		PrevKv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID), Value: data1Json},
		Kv:     &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID), Value: data2Json, CreateRevision: 1, ModRevision: 2}}

	testMapRows := func(isV1 bool) {
		var expRow *ovsjson.RowUpdate

		if isV1 {
			expRow = &ovsjson.RowUpdate{Old: &map[string]interface{}{mapColName: libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"k1": "v1", "k2": "v2", "k3": "v3"}}},
				New: &map[string]interface{}{"c1": "v1", mapColName: libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"k1": "v1", "k2": "v3", "k4": "v4"}}}}
		} else {
			expRow = &ovsjson.RowUpdate{Modify: &map[string]interface{}{mapColName: libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"k2": "v3", "k3": "v3", "k4": "v4"}}}}
		}
		updater := mcrToUpdater(ovsjson.MonitorCondRequest{}, "", tableSchema, isV1, log)
		validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)
	}
	testMapRows(true)
	testMapRows(false)
}

func TestMonitorPrepareModifySetRow(t *testing.T) {
	tableSchema := createTestTableSchema()
	expectedUUID := guuid.NewString()
	setColName := setColumn + "0"
	colSet := libovsdb.OvsSet{GoSet: []interface{}{"e1", "e2"}}
	data := map[string]interface{}{"c1": "v1", setColName: colSet}
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
	data1Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	colSet = libovsdb.OvsSet{GoSet: []interface{}{"e4", "e2"}}
	data[setColName] = colSet
	data2Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	event := &clientv3.Event{Type: mvccpb.PUT,
		PrevKv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID), Value: data1Json},
		Kv:     &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/" + expectedUUID), Value: data2Json, CreateRevision: 1, ModRevision: 2}}

	testSetRows := func(isV1 bool) {
		var expRow *ovsjson.RowUpdate

		if isV1 {
			expRow = &ovsjson.RowUpdate{Old: &map[string]interface{}{setColName: libovsdb.OvsSet{GoSet: []interface{}{"e1", "e2"}}},
				New: &map[string]interface{}{"c1": "v1", setColName: libovsdb.OvsSet{GoSet: []interface{}{"e2", "e4"}}}}
		} else {
			expRow = &ovsjson.RowUpdate{Modify: &map[string]interface{}{setColName: libovsdb.OvsSet{GoSet: []interface{}{"e1", "e4"}}}}
		}
		updater := mcrToUpdater(ovsjson.MonitorCondRequest{}, "", tableSchema, isV1, log)
		validateRowNotification(t, updater, event, expectedUUID, expRow, tableSchema)
	}
	testSetRows(true)
	testSetRows(false)
}

func validateRowNotification(t *testing.T, updater *updater, event *clientv3.Event, expectedUUID string, expRow *ovsjson.RowUpdate, tableSchema *libovsdb.TableSchema) {
	ovsdbEvent, err := etcd2ovsdbEvent(event, klogr.New())
	assert.Nil(t, err)
	row, uuid, err := updater.prepareRowNotification(ovsdbEvent)
	assert.Nil(t, err)
	assert.Equal(t, expectedUUID, uuid)
	rowsAreEqual(t, expRow, row, tableSchema)
}

func TestMonitorModifyRowMap(t *testing.T) {

	const modify = "modify"

	type operation map[string]struct {
		event        clientv3.Event
		expRowUpdate *ovsjson.RowUpdate
		err          error
	}

	data := map[string]interface{}{}
	data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: guuid.NewString()}
	goMap := map[string]interface{}{}
	goMap["theSame"] = "v1"
	goMap["newKey"] = "v1"
	goMap["newValue"] = "v1"
	newColMap, err := libovsdb.NewOvsMap(goMap)
	assert.Nil(t, err, "creation ovsMap")
	data["map"] = newColMap

	newData, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	goMap["newValue"] = "v2"
	goMap["removedKey"] = "v2"
	delete(goMap, "newKey")
	goDeltaMap := map[string]interface{}{}
	goDeltaMap["newKey"] = "v1"
	goDeltaMap["newValue"] = "v2"
	goDeltaMap["removedKey"] = "v2"
	deltaMap, err := libovsdb.NewOvsMap(goDeltaMap)
	assert.Nil(t, err, "creation ovsMap")

	oldColMap, err := libovsdb.NewOvsMap(goMap)
	assert.Nil(t, err, "creation ovsMap")
	data["map"] = oldColMap

	oldData, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	var tableSchema libovsdb.TableSchema
	tableSchema.Columns = map[string]*libovsdb.ColumnSchema{}
	columnType := libovsdb.ColumnType{Key: &libovsdb.BaseType{Type: "string"}, Value: &libovsdb.BaseType{Type: "string"}}
	columnSchema := libovsdb.ColumnSchema{Type: libovsdb.TypeMap, TypeObj: &columnType}
	tableSchema.Columns["map"] = &columnSchema

	tests := map[string]struct {
		updater updater
		op      operation
	}{"allColumns-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", &tableSchema, true, log),
		op: operation{modify: {event: clientv3.Event{Type: mvccpb.PUT,
			PrevKv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "table/000"), Value: oldData},
			Kv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/uuid"),
				Value: newData, CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: &ovsjson.RowUpdate{
				Old: &map[string]interface{}{"map": deltaMap},
				New: &map[string]interface{}{"map": newColMap}}}}},
		"allColumns-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", &tableSchema, false, log),
			op: operation{modify: {event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/000"), Value: oldData},
				Kv:     &mvccpb.KeyValue{Key: []byte(keyPrefix + "/db/table/000"), Value: newData, CreateRevision: 1, ModRevision: 2}},
				expRowUpdate: &ovsjson.RowUpdate{
					Modify: &map[string]interface{}{"map": deltaMap}}}}},
	}
	for name, ts := range tests {
		updater := ts.updater
		for opName, op := range ts.op {
			ovsdbEvent, err := etcd2ovsdbEvent(&op.event, klogr.New())
			assert.Nil(t, err)
			row, _, err := updater.prepareRowNotification(ovsdbEvent)
			if op.err != nil {
				assert.EqualErrorf(t, err, op.err.Error(), "[%s-%s test] expected error %s, got %v", name, opName, op.err.Error(), err)
				continue
			} else {
				assert.Nilf(t, err, "[%s-%s test] returned unexpected error %v", name, opName, err)
			}
			if op.expRowUpdate == nil {
				assert.Nilf(t, row, "[%s-%s test] returned unexpected row %#v", name, opName, row)
			} else {
				assert.NotNil(t, row, "[%s-%s test] returned nil row", name, opName)
				if updater.isV1 {
					ok, msg := row.ValidateRowUpdate()
					assert.Truef(t, ok, "[%s-%s test]  Row update is not valid %s %#v", name, opName, msg, row)
				} else {
					ok, msg := row.ValidateRowUpdate2()
					assert.Truef(t, ok, "[%s-%s test]  Row update is not valid %s %#v", name, opName, msg, row)
				}
				//fmt.Printf("op.expRowUpdate %+v\n", op.expRowUpdate)
				//fmt.Printf("row %+v\n", row)
				// TODO add compare
			}
		}
	}
}

func TestMonitorAddRemoveMonitor(t *testing.T) {
	const (
		databaseSchemaName       = "OVN_Northbound"
		databaseSchemaVer        = "5.31.0"
		schemaLogicalRouterTable = "Logical_Router"
		schemaNBGlobalTable      = "NB_Global"
		schemaACLTable           = "ACL"
		monid                    = "monid"
	)
	var (
		columnsNameKey = map[string]*libovsdb.ColumnSchema{
			"name": {
				Type: libovsdb.TypeString,
			},
			"key2": {
				Type: libovsdb.TypeInteger,
			},
		}
		columnsPriority = map[string]*libovsdb.ColumnSchema{
			"priority": {
				Type: libovsdb.TypeString,
			},
		}
	)
	var testSchemaSimple = &libovsdb.DatabaseSchema{
		Name:    databaseSchemaName,
		Version: databaseSchemaVer,
		Tables: map[string]libovsdb.TableSchema{
			schemaLogicalRouterTable: {
				Columns: columnsNameKey,
			},
			schemaNBGlobalTable: {
				Columns: columnsNameKey,
			},
			schemaACLTable: {
				Columns: columnsPriority,
			},
		},
	}

	expKey2Updaters := Key2Updaters{}
	schemas := libovsdb.Schemas{}
	schemas[databaseSchemaName] = testSchemaSimple
	db := DatabaseMock{Response: schemas}
	ctx := context.Background()
	handler := NewHandler(ctx, &db, nil, log)
	expMsg, err := json.Marshal([]interface{}{monid, databaseSchemaName})
	assert.Nil(t, err)
	updateExpected := func(databaseSchemaName string, tableSchemaName string, columns *[]string, jsonValue interface{}, isV1 bool) {
		key := common.NewTableKey(databaseSchemaName, tableSchemaName)
		tableSchema := libovsdb.TableSchema{Columns: schemas[databaseSchemaName].Tables[tableSchemaName].Columns}
		mcr := ovsjson.MonitorCondRequest{Columns: columns}
		expKey2Updaters[key] = []updater{*mcrToUpdater(mcr, jsonValueToString(jsonValue), &tableSchema, isV1, handler.log)}
	}

	jrpcServerMock := jrpcServerMock{
		expMethod:  MonitorCanceled,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	// add First monitor
	msg := fmt.Sprintf(`["%s",null,{"%s":[{"columns":[%s]}],"%s":[{"columns":[%s]}]}]`, databaseSchemaName, schemaLogicalRouterTable, "\"name\"", schemaNBGlobalTable, "")
	var params []interface{}
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	_, err = handler.addMonitor(params, ovsjson.Update)
	assert.Nil(t, err)
	monitor, ok := handler.monitors[databaseSchemaName]
	assert.True(t, ok)
	assert.Equal(t, handler, monitor.handler)
	assert.Equal(t, databaseSchemaName, monitor.dataBaseName)
	updateExpected(databaseSchemaName, schemaLogicalRouterTable, &[]string{"name"}, nil, true)
	updateExpected(databaseSchemaName, schemaNBGlobalTable, &[]string{}, nil, true)
	assert.Equal(t, expKey2Updaters, monitor.key2Updaters)
	cloned := cloneKey2Updaters(monitor.key2Updaters)

	// add second monitor
	msg = fmt.Sprintf(`["%s",["%s","%s"],{"%s":[{"columns":[%s]}]}]`, databaseSchemaName, monid, databaseSchemaName, schemaACLTable, "\"priority\"")
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	_, err = handler.addMonitor(params, ovsjson.Update2)
	assert.Nil(t, err)
	updateExpected(databaseSchemaName, schemaACLTable, &[]string{"priority"}, []interface{}{monid, databaseSchemaName}, false)
	assert.Equal(t, expKey2Updaters, monitor.key2Updaters)

	// remove the second monitor
	err = handler.removeMonitor(params[1], true)
	assert.Nil(t, err)
	assert.Equal(t, cloned, monitor.key2Updaters)

	expMsg, err = json.Marshal(nil)
	assert.Nil(t, err)
	jrpcServerMock.expMessage = expMsg

	// remove the first monitor
	err = handler.removeMonitor(nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(monitor.key2Updaters))
	assert.Equal(t, 0, len(handler.monitors))
}

func TestMonitorParseCMPJsonValueNilMCRArray(t *testing.T) {
	msg := `["OVN_Northbound",null,{"Logical_Router":[{"columns":["name"]}],"NB_Global":[{"columns":[]}]},"00000000-0000-0000-0000-000000000000"]`
	var params []interface{}
	err := json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	cmpr, err := parseCondMonitorParameters(params)
	assert.Nil(t, err)
	assert.Equal(t, cmpr.DatabaseName, "OVN_Northbound")
	assert.Nil(t, cmpr.JsonValue)
	mcrs := map[string][]ovsjson.MonitorCondRequest{}
	mcrs["Logical_Router"] = []ovsjson.MonitorCondRequest{{Columns: &[]string{"name"}}}
	mcrs["NB_Global"] = []ovsjson.MonitorCondRequest{{Columns: &[]string{}}}
	assert.EqualValues(t, cmpr.MonitorCondRequests, mcrs)
	assert.Equal(t, *cmpr.LastTxnID, "00000000-0000-0000-0000-000000000000")
}

func TestMonitorParseCMPMCR(t *testing.T) {
	msg := `["OVN_Northbound",["monid","OVN_Northbound"],{"Logical_Router":{"columns":["name"]},"NB_Global":{"columns":[]}}]`
	var params []interface{}
	err := json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	cmpr, err := parseCondMonitorParameters(params)
	assert.Nil(t, err)
	assert.Equal(t, cmpr.DatabaseName, "OVN_Northbound")
	assert.EqualValues(t, cmpr.JsonValue, []interface{}{"monid", "OVN_Northbound"})
	mcrs := map[string][]ovsjson.MonitorCondRequest{}
	mcrs["Logical_Router"] = []ovsjson.MonitorCondRequest{{Columns: &[]string{"name"}}}
	mcrs["NB_Global"] = []ovsjson.MonitorCondRequest{{Columns: &[]string{}}}
	assert.EqualValues(t, cmpr.MonitorCondRequests, mcrs)
	assert.Nil(t, cmpr.LastTxnID)
}

func TestMonitorNotifications1(t *testing.T) {
	handler, events := initHandler(t, `null`, ovsjson.Update)
	var wg sync.WaitGroup
	wg.Add(1)
	jrpcServerMock := jrpcServerMock{
		expMethod: Update,
		t:         t,
		wg:        &wg,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	handler.startNotifier(jsonValueToString(nil))
	monitor := handler.monitors[dbName]
	monitor.notify(etcdEvents2OvsdbEvents(t, events), 1)
	wg.Wait()
}

func TestMonitorNotifications2(t *testing.T) {
	handler, events := initHandler(t, `null`, ovsjson.Update2)
	var wg sync.WaitGroup
	wg.Add(1)
	jrpcServerMock := jrpcServerMock{
		expMethod: Update2,
		t:         t,
		wg:        &wg,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	handler.startNotifier(jsonValueToString(nil))
	monitor := handler.monitors[dbName]
	monitor.notify(etcdEvents2OvsdbEvents(t, events), 1)
	wg.Wait()
}

func TestMonitorNotifications3(t *testing.T) {
	handler, events := initHandler(t, `null`, ovsjson.Update3)
	var wg sync.WaitGroup
	wg.Add(1)
	jrpcServerMock := jrpcServerMock{
		expMethod: Update3,
		t:         t,
		wg:        &wg,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	handler.startNotifier(jsonValueToString(nil))
	monitor := handler.monitors[dbName]
	monitor.notify(etcdEvents2OvsdbEvents(t, events), 1)
	wg.Wait()
}

func isEqualMaps(t *testing.T, m1 *map[string]interface{}, m2 *map[string]interface{}, tableSchema *libovsdb.TableSchema) {
	if m1 == nil {
		assert.Nil(t, m2)
		return
	}
	if m2 == nil {
		assert.Nil(t, m1)
		return
	}
	err := tableSchema.Unmarshal(m1)
	assert.Nil(t, err)
	err = tableSchema.Unmarshal(m2)
	assert.Nil(t, err)
	for columnName, columnSchema := range tableSchema.Columns {
		if columnSchema.Type == libovsdb.TypeSet {
			set1, ok := (*m1)[columnName]
			set2, ok2 := (*m1)[columnName]
			if !ok || !ok2 {
				assert.True(t, ok == ok2)
				continue
			}
			ovsdbSet1 := set1.(libovsdb.OvsSet)
			ovsdbSet2 := set2.(libovsdb.OvsSet)
			assert.ElementsMatch(t, ovsdbSet1.GoSet, ovsdbSet2.GoSet)
		} else {
			assert.EqualValues(t, (*m1)[columnName], (*m2)[columnName])
		}
	}
}

func rowsAreEqual(t *testing.T, expRow *ovsjson.RowUpdate, row *ovsjson.RowUpdate, tableSchema *libovsdb.TableSchema) {
	if row == nil && expRow == nil {
		return
	}
	isEqualMaps(t, expRow.New, row.New, tableSchema)
	isEqualMaps(t, expRow.Old, row.Old, tableSchema)
	isEqualMaps(t, expRow.Initial, row.Initial, tableSchema)
	isEqualMaps(t, expRow.Insert, row.Insert, tableSchema)
	isEqualMaps(t, expRow.Modify, row.Modify, tableSchema)
	assert.Equal(t, expRow.Delete, row.Delete)
}

func initHandler(t *testing.T, jsonValue string, notificationType ovsjson.UpdateNotificationType) (*Handler, []*clientv3.Event) {

	columnSchema := libovsdb.ColumnSchema{Type: libovsdb.TypeString}
	var testSchemaSimple = &libovsdb.DatabaseSchema{
		Name: dbName,
		Tables: map[string]libovsdb.TableSchema{
			tableName: {Columns: map[string]*libovsdb.ColumnSchema{"c1": &columnSchema, "c2": &columnSchema}},
		},
	}
	schemas := libovsdb.Schemas{}
	schemas[dbName] = testSchemaSimple
	msg := `["dbName",` + jsonValue + `,{"T1":[{"columns":["c1","c2"]}]}]`
	row := map[string]interface{}{"c1": "v1", "c2": "v2"}
	uuid := common.GenerateUUID()
	row[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: uuid}
	dataJson := prepareData(t, row, true)
	keyStr := fmt.Sprintf("%s/%s/%s/%s", keyPrefix, dbName, tableName, uuid)
	events := []*clientv3.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte(keyStr), Value: dataJson, CreateRevision: 1, ModRevision: 1}}}

	db := DatabaseMock{Response: schemas}
	ctx := context.Background()
	handler := NewHandler(ctx, &db, nil, klogr.New())

	var params []interface{}
	err := json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	_, err = handler.addMonitor(params, notificationType)
	assert.Nil(t, err)

	_, ok := handler.monitors[dbName]
	assert.True(t, ok)
	return handler, events
}

func prepareData(t *testing.T, data map[string]interface{}, withUUID bool) []byte {
	if withUUID {
		data[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: rowUUID}
	}
	dataJson, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)
	return dataJson
}

func cloneKey2Updaters(key2Updaters Key2Updaters) Key2Updaters {
	newMap := Key2Updaters{}
	for k, v := range key2Updaters {
		updaters := make([]updater, 0, len(v))
		for _, u := range v {
			updaters = append(updaters, u)
		}
		newMap[k] = updaters
	}
	return newMap
}

type jrpcServerMock struct {
	expMessage interface{}
	expMethod  string
	t          *testing.T
	wg         *sync.WaitGroup
}

func (j *jrpcServerMock) Wait() error {
	return nil
}

func (j *jrpcServerMock) Stop() {}

func (j *jrpcServerMock) Notify(_ context.Context, method string, message interface{}) error {
	assert.NotNil(j.t, method)
	assert.Equal(j.t, j.expMethod, method)
	//assert.Equal(j.t, j.expMessage, message)
	if j.wg != nil {
		j.wg.Done()
	}
	return nil
}

func TestSetsDifferenceEquals(t *testing.T) {
	set1 := libovsdb.OvsSet{GoSet: []interface{}{"one", "two"}}
	set2 := libovsdb.OvsSet{GoSet: []interface{}{"two", "one"}}
	expectDiff := libovsdb.OvsSet{}

	diff := setsDifference(set1, set2)
	assert.Equal(t, expectDiff, diff)
}

func TestSetsDifferenceSubset1(t *testing.T) {
	set1 := libovsdb.OvsSet{GoSet: []interface{}{"one", "two", "three"}}
	set2 := libovsdb.OvsSet{GoSet: []interface{}{"two", "one"}}
	expectDiff := libovsdb.OvsSet{GoSet: []interface{}{"three"}}

	diff := setsDifference(set1, set2)
	assert.Equal(t, expectDiff, diff)
}

func TestSetsDifferenceSubset2(t *testing.T) {
	set1 := libovsdb.OvsSet{GoSet: []interface{}{"one", "two"}}
	set2 := libovsdb.OvsSet{GoSet: []interface{}{"two", "three", "one"}}
	expectDiff := libovsdb.OvsSet{GoSet: []interface{}{"three"}}

	diff := setsDifference(set1, set2)
	assert.Equal(t, expectDiff, diff)
}

// the test violates missing duplication elements, but it passes due to duplications are in the second test only.
func TestSetsDifferenceDifferentSets(t *testing.T) {
	set1 := libovsdb.OvsSet{GoSet: []interface{}{"one", "two", "four"}}
	set2 := libovsdb.OvsSet{GoSet: []interface{}{"two", "three", "two"}}
	expectDiff := libovsdb.OvsSet{GoSet: []interface{}{"one", "three", "four"}}

	diff := setsDifference(set1, set2)
	assert.ElementsMatch(t, expectDiff.GoSet, diff.GoSet)
}

func TestMonitorCondChange(t *testing.T) {
	const (
		dbName    = "dbName"
		tableName = "T1"
	)
	dataRow := map[string]interface{}{"c1": "v1", "c2": "v2"}
	var row, emptyRow map[string]interface{}
	columnSchema := libovsdb.ColumnSchema{Type: libovsdb.TypeString}
	tableSchema := libovsdb.TableSchema{Columns: map[string]*libovsdb.ColumnSchema{"c1": &columnSchema, "c2": &columnSchema}}
	handler, _ := initHandler(t, `null`, ovsjson.Update)
	monitorCondChange := func(mcrs map[string][]ovsjson.MonitorCondRequest) {
		ctx := context.Background()
		params := []interface{}{"<nil>", "<nil>", mcrs}
		_, err := handler.MonitorCondChange(ctx, params)
		assert.Nil(t, err)
	}
	addUuidToRow := func(rowIn map[string]interface{}) (rowOut map[string]interface{}) {
		expectedUUID := rowUUID
		rowIn[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: expectedUUID}
		return rowIn
	}
	handlerCallToPrepareRow := func(rowInWithoutUUID map[string]interface{}) map[string]interface{} {
		rowInWithUUID := addUuidToRow(rowInWithoutUUID)
		row := &libovsdb.Row{Fields: rowInWithUUID}
		key := common.NewTableKey(dbName, tableName)
		updater := handler.monitors[dbName].key2Updaters[key][0]
		rowOut, _, err := updater.prepareRow(row)
		assert.Nilf(t, err, " prepareRow threw %v", err)
		return rowOut
	}
	monitorCondChange(map[string][]ovsjson.MonitorCondRequest{"T1": {{Where: &[]interface{}{false}}}})
	row = handlerCallToPrepareRow(dataRow)
	isEqualMaps(t, &emptyRow, &row, &tableSchema)
	monitorCondChange(map[string][]ovsjson.MonitorCondRequest{"T1": {{Where: &[]interface{}{true}}}})
	row = handlerCallToPrepareRow(dataRow)
	isEqualMaps(t, &dataRow, &row, &tableSchema)
}

func etcdEvents2OvsdbEvents(t *testing.T, events []*clientv3.Event) []*ovsdbNotificationEvent {
	ovsdbEvents := make([]*ovsdbNotificationEvent, 0, len(events))
	for _, event := range events {
		ovsdbEvent, err := etcd2ovsdbEvent(event, log)
		assert.Nil(t, err)
		ovsdbEvents = append(ovsdbEvents, ovsdbEvent)
	}
	return ovsdbEvents
}
