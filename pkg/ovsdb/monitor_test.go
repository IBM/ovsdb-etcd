package ovsdb

import (
	"context"
	"encoding/json"
	"testing"

	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

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
	data[COL_UUID] = libovsdb.UUID{GoUUID: guuid.NewString()}
	data1Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	data["c2"] = "v3"
	data2Json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)

	tests := map[string]struct {
		updater updater
		op      operation
	}{"allColumns-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", true),
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
		"SingleColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, "", true),
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
		"ZeroColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, "", true),
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

		"allColumns-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, "", false),
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
		"SingleColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, "", false),
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
		"ZeroColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, "", false),
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

func TestMonitorAddRemoveMonitor(t *testing.T) {
	db, _ := NewDatabaseMock()
	ctx := context.Background()
	handler := NewHandler(ctx, db, nil)
	expMsg, err := json.Marshal([]interface{}{"monid", "OVN_Northbound"})
	assert.Nil(t, err)
	jrpcServerMock := jrpcServerMock{
		expMethod:  MONITOR_CANCELED,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	// add First monitor
	msg := `["OVN_Northbound",null,{"Logical_Router":[{"columns":["name"]}],"NB_Global":[{"columns":[]}]}]`
	var params []interface{}
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	handler.addMonitor(params, ovsjson.Update)
	monitor, ok := handler.monitors["OVN_Northbound"]
	assert.True(t, ok)
	assert.Equal(t, handler, monitor.handler)
	assert.Equal(t, "OVN_Northbound", monitor.dataBaseName)
	expKey2Updaters := Key2Updaters{}
	key := common.NewTableKey("OVN_Northbound", "Logical_Router")
	expKey2Updaters[key] = []updater{{
		Columns:          map[string]bool{"name": true},
		Where:            nil,
		Select:           libovsdb.MonitorSelect{},
		isV1:             true,
		notificationType: 0,
		jasonValueStr:    jsonValueToString(nil),
	}}
	key = common.NewTableKey("OVN_Northbound", "NB_Global")
	expKey2Updaters[key] = []updater{{
		Columns:          map[string]bool{},
		Where:            nil,
		Select:           libovsdb.MonitorSelect{},
		isV1:             true,
		notificationType: 0,
		jasonValueStr:    jsonValueToString(nil),
	}}
	assert.Equal(t, expKey2Updaters, monitor.key2Updaters)
	cloned := cloneKey2Updaters(monitor.key2Updaters)

	// add second monitor
	msg = `["OVN_Northbound",["monid","OVN_Northbound"],{"ACL":[{"columns":["priority"]}]}]`
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	handler.addMonitor(params, ovsjson.Update2)
	key = common.NewTableKey("OVN_Northbound", "ACL")
	expKey2Updaters[key] = []updater{{
		Columns:          map[string]bool{"priority": true},
		Where:            nil,
		Select:           libovsdb.MonitorSelect{},
		isV1:             false,
		notificationType: 0,
		jasonValueStr:    jsonValueToString([]interface{}{"monid", "OVN_Northbound"}),
	}}
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
	handler := initHandler(t)
	row := map[string]interface{}{"c1": "v1", "c2": "v2"}
	dataJson := prepareData(t, row, true)

	events := []*clientv3.Event{
		&clientv3.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T1/000"),
			Value: dataJson, CreateRevision: 1, ModRevision: 1}}}

	tableUpdates := ovsjson.TableUpdates{}
	tableUpdate := ovsjson.TableUpdate{}
	delete(row, COL_UUID)
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
	monitor := handler.monitors[DB_NAME]
	monitor.notify(events)
}

func TestMonitorNotifications2(t *testing.T) {
	handler := initHandler(t)
	row := map[string]interface{}{"c1": "v1", "c2": "v2"}
	dataJson := prepareData(t, row, true)

	events := []*clientv3.Event{
		&clientv3.Event{Type: mvccpb.DELETE,
			PrevKv: &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T2/000"), Value: dataJson},
			Kv:     &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T2/000")}},
	}
	tableUpdates := ovsjson.TableUpdates{}
	tableUpdate := ovsjson.TableUpdate{}
	rowUpdate := ovsjson.RowUpdate{Delete: true}
	tableUpdate[ROW_UUID] = rowUpdate
	tableUpdates["T2"] = tableUpdate
	expMsg, err := json.Marshal([]interface{}{[]interface{}{"monid", "update2"}, tableUpdates})
	assert.Nil(t, err)

	jrpcServerMock := jrpcServerMock{
		expMethod:  UPDATE2,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	monitor := handler.monitors[DB_NAME]
	monitor.notify(events)
}

func TestMonitorNotifications3(t *testing.T) {
	handler := initHandler(t)
	row1 := map[string]interface{}{"c1": "v1", "c2": "v2"}
	data1Json := prepareData(t, row1, true)
	row2 := map[string]interface{}{"c2": "v3"}
	data2Json := prepareData(t, row2, true)

	events := []*clientv3.Event{
		&clientv3.Event{Type: mvccpb.PUT,
			PrevKv: &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T3/000"), Value: data1Json},
			Kv:     &mvccpb.KeyValue{Key: []byte("ovsdb/nb/dbName/T3/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
	}
	tableUpdates := ovsjson.TableUpdates{}
	tableUpdate := ovsjson.TableUpdate{}
	delete(row2, COL_UUID)
	rowUpdate := ovsjson.RowUpdate{Modify: &row2}
	tableUpdate[ROW_UUID] = rowUpdate
	tableUpdates["T3"] = tableUpdate
	expMsg, err := json.Marshal([]interface{}{[]interface{}{"monid", "update3"}, LAST_TNX, tableUpdates})
	assert.Nil(t, err)

	jrpcServerMock := jrpcServerMock{
		expMethod:  UPDATE3,
		expMessage: expMsg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	monitor := handler.monitors[DB_NAME]
	monitor.notify(events)
}

func initHandler(t *testing.T) *Handler {
	common.SetPrefix("ovsdb/nb")
	db, _ := NewDatabaseMock()
	ctx := context.Background()
	handler := NewHandler(ctx, db, nil)

	msg := `["dbName", null ,{"T1":[{"columns":[]}]}]`
	var params []interface{}
	err := json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	handler.addMonitor(params, ovsjson.Update)

	msg = `["dbName", ["monid","update2"],{"T2":[{"columns":[]}]}]`
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	handler.addMonitor(params, ovsjson.Update2)

	msg = `["dbName", ["monid","update3"], {"T3":[{"columns":[]}]}, "00000000-0000-0000-0000-000000000000"]`
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	handler.addMonitor(params, ovsjson.Update3)

	_, ok := handler.monitors[DB_NAME]
	assert.True(t, ok)
	return handler
}

func prepareData(t *testing.T, data map[string]interface{}, withUUID bool) []byte {
	if withUUID {
		data[COL_UUID] = libovsdb.UUID{GoUUID: ROW_UUID}
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
