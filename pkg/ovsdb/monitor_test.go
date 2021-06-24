package ovsdb

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"testing"

	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	klog "k8s.io/klog/v2"
	klogr "k8s.io/klog/v2/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

func init() {
	fs := flag.NewFlagSet("fs", flag.PanicOnError)
	klog.InitFlags(fs)
	fs.Set("v", "10")
}

func TestMonitorRowUpdate(t *testing.T) {

	const (
		PUT    = "put"
		DELETE = "delete"
		MODIFY = "modify"
	)

	type operation map[string]struct {
		event        clientv3.Event
		expRowUpdate *ovsjson.RowUpdate
		err          error
	}

	data := map[string]interface{}{"c1": "v1", "c2": "v2"}
	data[libovsdb.COL_UUID] = libovsdb.UUID{GoUUID: guuid.NewString()}
	data1Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	data["c2"] = "v3"
	data2Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	var tableSchema libovsdb.TableSchema
	tableSchema.Columns = map[string]*libovsdb.ColumnSchema{}
	columnSchema := libovsdb.ColumnSchema{Type: libovsdb.TypeString}
	tableSchema.Columns["c1"] = &columnSchema
	tableSchema.Columns["c2"] = &columnSchema
	tableSchema.Columns["c3"] = &columnSchema

	tests := map[string]struct {
		updater updater
		op      operation
	}{"allColumns-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", &tableSchema, true),
		op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
			Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"),
				Value: data1Json, CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: &ovsjson.RowUpdate{New: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
			DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"),
					Value: data1Json},
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
				expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
			MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"),
					Value: data2Json, CreateRevision: 1, ModRevision: 2}},
				expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}, New: &map[string]interface{}{"c1": "v1", "c2": "v3"}}}}},
		"SingleColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, "", &tableSchema, true),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"),
					Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: &ovsjson.RowUpdate{New: &map[string]interface{}{"c2": "v2"}}},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}, New: &map[string]interface{}{"c2": "v3"}}}}},
		"ZeroColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, "", &tableSchema, true),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: nil},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: nil},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: nil}}},

		"allColumns-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", &tableSchema, false),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: &ovsjson.RowUpdate{Insert: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Delete: true}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: &ovsjson.RowUpdate{Modify: &map[string]interface{}{"c2": "v3"}}}}},
		"SingleColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, "", &tableSchema, false),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: &ovsjson.RowUpdate{Insert: &map[string]interface{}{"c2": "v2"}}},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Delete: true}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: &ovsjson.RowUpdate{Modify: &map[string]interface{}{"c2": "v3"}}}}},
		"ZeroColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, "", &tableSchema, false),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: nil},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Delete: true}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: nil}}},
	}
	for name, ts := range tests {
		updater := ts.updater
		for opName, op := range ts.op {
			row, _, err := updater.prepareRowUpdate(&op.event)
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
				assert.EqualValuesf(t, op.expRowUpdate, row, "[%s-%s test] returned wrong row update, expected %#v, got %#v", name, opName, *op.expRowUpdate, *row)
			}
		}
	}
}

func TestMonitorModifyRowMap(t *testing.T) {

	const MODIFY = "modify"

	type operation map[string]struct {
		event        clientv3.Event
		expRowUpdate *ovsjson.RowUpdate
		err          error
	}

	data := map[string]interface{}{}
	data[libovsdb.COL_UUID] = libovsdb.UUID{GoUUID: guuid.NewString()}
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
	}{"allColumns-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", &tableSchema, true),
		op: operation{MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
			PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: oldData},
			Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"),
				Value: newData, CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: &ovsjson.RowUpdate{
				Old: &map[string]interface{}{"map": deltaMap},
				New: &map[string]interface{}{"map": newColMap}}}}},
		"allColumns-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", &tableSchema, false),
			op: operation{MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: oldData},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: newData, CreateRevision: 1, ModRevision: 2}},
				expRowUpdate: &ovsjson.RowUpdate{
					Modify: &map[string]interface{}{"map": deltaMap}}}}},
	}
	for name, ts := range tests {
		updater := ts.updater
		for opName, op := range ts.op {
			row, _, err := updater.prepareRowUpdate(&op.event)
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
		databaseSchemaName           = "OVN_Northbound"
		databaseSchemaVer            = "5.31.0"
		logicalRouterTableSchemaName = "Logical_Router"
		NB_GlobalTableSchemaName     = "NB_Global"
		ACL_TableSchemaName          = "ACL"
		monid                        = "monid"
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
	var testSchemaSimple *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
		Name:    databaseSchemaName,
		Version: databaseSchemaVer,
		Tables: map[string]libovsdb.TableSchema{
			logicalRouterTableSchemaName: {
				Columns: columnsNameKey,
			},
			NB_GlobalTableSchemaName: {
				Columns: columnsNameKey,
			},
			ACL_TableSchemaName: {
				Columns: columnsPriority,
			},
		},
	}

	expKey2Updaters := Key2Updaters{}
	schemas := libovsdb.Schemas{}
	updateExpected := func(databaseSchemaName string, tableSchemaName string, columns []string, jsonValue interface{}, isV1 bool) {
		key := common.NewTableKey(databaseSchemaName, tableSchemaName)
		tableSchema := libovsdb.TableSchema{Columns: schemas[databaseSchemaName].Tables[tableSchemaName].Columns}
		mcr := ovsjson.MonitorCondRequest{Columns: columns}
		expKey2Updaters[key] = []updater{*mcrToUpdater(mcr, jsonValueToString(jsonValue), &tableSchema, isV1)}
	}

	schemas[databaseSchemaName] = testSchemaSimple
	db := DatabaseMock{Response: schemas}
	ctx := context.Background()
	handler := NewHandler(ctx, &db, nil, klogr.New())
	expMsg, err := json.Marshal([]interface{}{monid, databaseSchemaName})
	assert.Nil(t, err)
	jrpcServerMock := jrpcServerMock{
		expMethod:  MONITOR_CANCELED,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	// add First monitor
	msg := fmt.Sprintf(`["%s",null,{"%s":[{"columns":[%s]}],"%s":[{"columns":[%s]}]}]`, databaseSchemaName, logicalRouterTableSchemaName, "\"name\"", NB_GlobalTableSchemaName, "")
	var params []interface{}
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	_, err = handler.addMonitor(params, ovsjson.Update)
	assert.Nil(t, err)
	monitor, ok := handler.monitors[databaseSchemaName]
	assert.True(t, ok)
	assert.Equal(t, handler, monitor.handler)
	assert.Equal(t, databaseSchemaName, monitor.dataBaseName)
	updateExpected(databaseSchemaName, logicalRouterTableSchemaName, []string{"name"}, nil, true)
	updateExpected(databaseSchemaName, NB_GlobalTableSchemaName, []string{}, nil, true)
	assert.Equal(t, expKey2Updaters, monitor.key2Updaters)
	cloned := cloneKey2Updaters(monitor.key2Updaters)

	// add second monitor
	msg = fmt.Sprintf(`["%s",["%s","%s"],{"%s":[{"columns":[%s]}]}]`, databaseSchemaName, monid, databaseSchemaName, ACL_TableSchemaName, "\"priority\"")
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	_, err = handler.addMonitor(params, ovsjson.Update2)
	assert.Nil(t, err)
	updateExpected(databaseSchemaName, ACL_TableSchemaName, []string{"priority"}, []interface{}{monid, databaseSchemaName}, false)
	assert.Equal(t, expKey2Updaters, monitor.key2Updaters)

	// remove the second monitor
	handler.removeMonitor(params[1], true)
	assert.Equal(t, cloned, monitor.key2Updaters)

	expMsg, err = json.Marshal(nil)
	assert.Nil(t, err)
	jrpcServerMock.expMessage = expMsg

	// remove the first monitor
	handler.removeMonitor(nil, true)
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
	mcrs["Logical_Router"] = []ovsjson.MonitorCondRequest{{Columns: []string{"name"}}}
	mcrs["NB_Global"] = []ovsjson.MonitorCondRequest{{Columns: []string{}}}
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
	mcrs["Logical_Router"] = []ovsjson.MonitorCondRequest{{Columns: []string{"name"}}}
	mcrs["NB_Global"] = []ovsjson.MonitorCondRequest{{Columns: []string{}}}
	assert.EqualValues(t, cmpr.MonitorCondRequests, mcrs)
	assert.Nil(t, cmpr.LastTxnID)
}

const (
	DB_NAME  = "dbName"
	ROW_UUID = "43f24179-432d-435b-a8dc-e7134cf39e32"
	LAST_TNX = "00000000-0000-0000-0000-000000000000"
)

func TestMonitorNotifications1(t *testing.T) {
	const (
		databaseSchemaName = "dbName"
		T1TableSchemaName  = "T1"
	)
	var testSchemaSimple *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
		Name: databaseSchemaName,
		Tables: map[string]libovsdb.TableSchema{
			T1TableSchemaName: {},
		},
	}
	schemas := libovsdb.Schemas{}
	schemas[databaseSchemaName] = testSchemaSimple
	jsonValue := `null`
	msg := `["dbName",` + jsonValue + `,{"T1":[{"columns":[]}]}]`
	handler := initHandler(t, schemas, msg, ovsjson.Update)
	row := map[string]interface{}{"c1": "v1", "c2": "v2"}
	dataJson := prepareData(t, row, true)

	events := []*clientv3.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T1/000"),
			Value: dataJson, CreateRevision: 1, ModRevision: 1}}}

	tableUpdates := ovsjson.TableUpdates{}
	tableUpdate := ovsjson.TableUpdate{}
	delete(row, libovsdb.COL_UUID)
	rowUpdate := ovsjson.RowUpdate{New: &row}
	tableUpdate[ROW_UUID] = rowUpdate
	tableUpdates["T1"] = tableUpdate
	expMsg, err := json.Marshal([]interface{}{nil, tableUpdates})
	assert.Nil(t, err)

	jrpcServerMock := jrpcServerMock{
		expMethod:  UPDATE,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	handler.startNotifier(jsonValueToString(nil))
	monitor := handler.monitors[DB_NAME]
	var wg sync.WaitGroup
	wg.Add(1)
	monitor.notify(events, 1, &wg)
	wg.Wait()
}

func TestMonitorNotifications2(t *testing.T) {
	const (
		databaseSchemaName = "dbName"
		T2TableSchemaName  = "T2"
	)
	var testSchemaSimple *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
		Name: databaseSchemaName,
		Tables: map[string]libovsdb.TableSchema{
			T2TableSchemaName: {},
		},
	}
	schemas := libovsdb.Schemas{}
	schemas[databaseSchemaName] = testSchemaSimple
	msg := `["dbName", ["monid","update2"],{"T2":[{"columns":[]}]}]`
	handler := initHandler(t, schemas, msg, ovsjson.Update2)
	jsonValue := []interface{}{"monid", "update2"}
	row := map[string]interface{}{"c1": "v1", "c2": "v2"}
	dataJson := prepareData(t, row, true)

	events := []*clientv3.Event{
		{Type: mvccpb.DELETE,
			PrevKv: &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T2/000"), Value: dataJson},
			Kv:     &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T2/000")}},
	}
	tableUpdates := ovsjson.TableUpdates{}
	tableUpdate := ovsjson.TableUpdate{}
	rowUpdate := ovsjson.RowUpdate{Delete: true}
	tableUpdate[ROW_UUID] = rowUpdate
	tableUpdates["T2"] = tableUpdate
	expMsg, err := json.Marshal([]interface{}{jsonValue, tableUpdates})
	assert.Nil(t, err)

	jrpcServerMock := jrpcServerMock{
		expMethod:  UPDATE2,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	handler.startNotifier(jsonValueToString(jsonValue))
	monitor := handler.monitors[DB_NAME]
	var wg sync.WaitGroup
	wg.Add(1)
	monitor.notify(events, 2, &wg)
	wg.Wait()
}

func TestMonitorNotifications3(t *testing.T) {
	const (
		databaseSchemaName = "dbName"
		T3TableSchemaName  = "T3"
	)
	var testSchemaSimple *libovsdb.DatabaseSchema = &libovsdb.DatabaseSchema{
		Name: databaseSchemaName,
		Tables: map[string]libovsdb.TableSchema{
			T3TableSchemaName: {},
		},
	}
	schemas := libovsdb.Schemas{}
	schemas[databaseSchemaName] = testSchemaSimple
	msg := `["dbName",["monid","update3"], {"T3":[{"columns":[]}]}, "00000000-0000-0000-0000-000000000000"]`
	jsonValue := []interface{}{"monid", "update3"}
	handler := initHandler(t, schemas, msg, ovsjson.Update3)
	row1 := map[string]interface{}{"c1": "v1", "c2": "v2"}
	data1Json := prepareData(t, row1, true)
	row2 := map[string]interface{}{"c2": "v3"}
	data2Json := prepareData(t, row2, true)

	events := []*clientv3.Event{
		{Type: mvccpb.PUT,
			PrevKv: &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T3/000"), Value: data1Json},
			Kv:     &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T3/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
	}
	tableUpdates := ovsjson.TableUpdates{}
	tableUpdate := ovsjson.TableUpdate{}
	delete(row2, libovsdb.COL_UUID)
	rowUpdate := ovsjson.RowUpdate{Modify: &row2}
	tableUpdate[ROW_UUID] = rowUpdate
	tableUpdates["T3"] = tableUpdate
	expMsg, err := json.Marshal([]interface{}{jsonValue, LAST_TNX, tableUpdates})
	assert.Nil(t, err)

	jrpcServerMock := jrpcServerMock{
		expMethod:  UPDATE3,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	handler.startNotifier(jsonValueToString(jsonValue))
	monitor := handler.monitors[DB_NAME]
	var wg sync.WaitGroup
	wg.Add(1)
	monitor.notify(events, 3, &wg)
	wg.Wait()
}

func initHandler(t *testing.T, schemas libovsdb.Schemas, msg string, notificationType ovsjson.UpdateNotificationType) *Handler {
	common.SetPrefix("ovsdb/nb")
	//db, _ := NewDatabaseMock()
	db := DatabaseMock{Response: schemas}
	ctx := context.Background()
	handler := NewHandler(ctx, &db, nil, klogr.New())

	var params []interface{}
	err := json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	_, err = handler.addMonitor(params, notificationType)
	assert.Nil(t, err)

	_, ok := handler.monitors[DB_NAME]
	assert.True(t, ok)
	return handler
}

func prepareData(t *testing.T, data map[string]interface{}, withUUID bool) []byte {
	if withUUID {
		data[libovsdb.COL_UUID] = libovsdb.UUID{GoUUID: ROW_UUID}
	}
	dataJson, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)
	return dataJson
}

func cloneKey2Updaters(key2Updaters Key2Updaters) Key2Updaters {
	newMap := Key2Updaters{}
	for k, v := range key2Updaters {
		updaters := []updater{}
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
}

func (j *jrpcServerMock) Wait() error {
	return nil
}

func (j *jrpcServerMock) Stop() {}

func (j *jrpcServerMock) Notify(ctx context.Context, method string, params interface{}) error {
	assert.NotNil(j.t, method)
	assert.Equal(j.t, j.expMethod, method)
	buf, err := json.Marshal(params)
	assert.Nil(j.t, err)
	assert.Equal(j.t, j.expMessage, buf)
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

func TestSetsDifferenceDifferentSets(t *testing.T) {
	set1 := libovsdb.OvsSet{GoSet: []interface{}{"one", "two", "four"}}
	set2 := libovsdb.OvsSet{GoSet: []interface{}{"two", "three", "two"}}
	expectDiff := libovsdb.OvsSet{GoSet: []interface{}{"one", "three", "four"}}

	diff := setsDifference(set1, set2)
	assert.ElementsMatch(t, expectDiff.GoSet, diff.GoSet)
}
