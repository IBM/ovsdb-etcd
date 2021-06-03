package ovsdb

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"

	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

func TestRowUpdate(t *testing.T) {

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
	}{"allColumns-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, true),
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
		"SingleColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, true),
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
		"ZeroColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, true),
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

		"allColumns-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, false),
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
		"SingleColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, false),
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
		"ZeroColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, false),
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

func initMonitor(prefix string, dbName string) *monitor {
	common.SetPrefix(prefix)
	return newMonitor(dbName, &DatabaseMock{})
}

func addTablesToMonitor(m *monitor, tables []string) []common.Key {
	keys := make([]common.Key, len(tables))
	for idx, table := range tables {
		keys[idx] = common.NewTableKey(m.dataBaseName, table)
	}
	return keys
}

func addNewHandler(m *monitor, handler_json_value string, k2u Key2Updaters) handlerKey {
	handler := handlerKey{jsonValue: handler_json_value}
	m.addUpdaters(k2u, handler)
	return handler
}

func newUpdater(columns []string, isV1 bool) *updater {
	return mcrToUpdater(ovsjson.MonitorCondRequest{Columns: columns}, isV1)
}

func assertEqualMonitors(t *testing.T, expected, actual *monitor) {
	assert.Equal(t, expected.key2Updaters, actual.key2Updaters, "Key to updater maps should be equals")
	assert.Equal(t, expected.updater2handlers, actual.updater2handlers, "Updaters to handlers maps should be equals")
}

func TestMonitorAddSingleHandler(t *testing.T) {
	m := initMonitor("ovsdb/nb", "dbtest")
	tables := []string{"table1", "table2"}
	keys := addTablesToMonitor(m, tables)
	updaters := []*updater{
		newUpdater([]string{"c1", "c3", "c2"}, true),
		newUpdater([]string{"c4"}, true),
		newUpdater([]string{"a1"}, true),
	}
	key2Updaters := Key2Updaters{keys[0]: {*updaters[0], *updaters[1]}, keys[1]: {*updaters[2]}}
	handlers := []handlerKey{addNewHandler(m, "jsonValue1", key2Updaters)}
	expected := &monitor{
		key2Updaters:     key2Updaters,
		updater2handlers: map[string][]handlerKey{updaters[0].key: {handlers[0]}, updaters[1].key: {handlers[0]}, updaters[2].key: {handlers[0]}},
	}
	assertEqualMonitors(t, expected, m)
}

func TestMonitorAddTwoSameHandlers(t *testing.T) {
	m := initMonitor("ovsdb/nb", "dbtest")
	tables := []string{"table1", "table2"}
	keys := addTablesToMonitor(m, tables)
	updaters := []*updater{
		newUpdater([]string{"c1", "c3", "c2"}, true),
		newUpdater([]string{"c4"}, true),
		newUpdater([]string{"a1"}, true),
	}
	key2Updaters := Key2Updaters{keys[0]: {*updaters[0], *updaters[1]}, keys[1]: {*updaters[2]}}
	handlers := []handlerKey{addNewHandler(m, "jsonValue1", key2Updaters),
		addNewHandler(m, "jsonValue2", key2Updaters)}
	expected := &monitor{
		key2Updaters:     key2Updaters,
		updater2handlers: map[string][]handlerKey{updaters[0].key: {handlers[0], handlers[1]}, updaters[1].key: {handlers[0], handlers[1]}, updaters[2].key: {handlers[0], handlers[1]}},
	}
	assertEqualMonitors(t, expected, m)
}

func TestMonitorAddThreeHandlers(t *testing.T) {
	m := initMonitor("ovsdb/nb", "dbtest")
	tables := []string{"table1", "table2"}
	keys := addTablesToMonitor(m, tables)
	updaters := []*updater{
		newUpdater([]string{"c1", "c3", "c2"}, true),
		newUpdater([]string{"c4"}, true),
		newUpdater([]string{"a1"}, true),
		newUpdater([]string{"c1", "c3", "c2"}, false),
	}
	key2Updaters := []Key2Updaters{{keys[0]: {*updaters[0], *updaters[1]}, keys[1]: {*updaters[2]}},
		{keys[0]: {*updaters[3]}}}
	handlers := []handlerKey{addNewHandler(m, "jsonValue1", key2Updaters[0]),
		addNewHandler(m, "jsonValue2", key2Updaters[0]),
		addNewHandler(m, "jsonValue3", key2Updaters[1])}
	expected := &monitor{
		key2Updaters: Key2Updaters{keys[0]: {*updaters[0], *updaters[1], *updaters[3]}, keys[1]: {*updaters[2]}},
		updater2handlers: map[string][]handlerKey{updaters[0].key: {handlers[0], handlers[1]},
			updaters[1].key: {handlers[0], handlers[1]},
			updaters[2].key: {handlers[0], handlers[1]},
			updaters[3].key: {handlers[2]}}}
	assertEqualMonitors(t, expected, m)
}

func TestMonitorRemoveHandlerT1(t *testing.T) {
	m := initMonitor("ovsdb/nb", "dbtest")
	tables := []string{"table1", "table2"}
	keys := addTablesToMonitor(m, tables)
	updaters := []*updater{
		newUpdater([]string{"c1", "c3", "c2"}, true),
		newUpdater([]string{"c4"}, true),
		newUpdater([]string{"a1"}, true),
		newUpdater([]string{"c1", "c3", "c2"}, false),
	}
	key2Updaters := []Key2Updaters{{keys[0]: {*updaters[0], *updaters[1]}, keys[1]: {*updaters[2]}},
		{keys[0]: {*updaters[3]}}}
	handlers := []handlerKey{addNewHandler(m, "jsonValue1", key2Updaters[0]),
		addNewHandler(m, "jsonValue2", key2Updaters[0]),
		addNewHandler(m, "jsonValue3", key2Updaters[1])}

	m.removeUpdaters(map[string][]string{tables[0]: {updaters[3].key}}, handlers[2])
	expected := &monitor{
		key2Updaters:     key2Updaters[0],
		updater2handlers: map[string][]handlerKey{updaters[0].key: {handlers[0], handlers[1]}, updaters[1].key: {handlers[0], handlers[1]}, updaters[2].key: {handlers[0], handlers[1]}},
	}
	assertEqualMonitors(t, expected, m)
}

func TestMonitorRemoveHandlerT2(t *testing.T) {
	m := initMonitor("ovsdb/nb", "dbtest")
	tables := []string{"table1", "table2"}
	keys := addTablesToMonitor(m, tables)
	updaters := []*updater{
		newUpdater([]string{"c1", "c3", "c2"}, true),
		newUpdater([]string{"c4"}, true),
		newUpdater([]string{"a1"}, true),
	}
	key2Updaters := Key2Updaters{keys[0]: {*updaters[0], *updaters[1]}, keys[1]: {*updaters[2]}}
	handlers := []handlerKey{addNewHandler(m, "jsonValue1", key2Updaters),
		addNewHandler(m, "jsonValue2", key2Updaters)}
	m.removeUpdaters(map[string][]string{tables[0]: {updaters[0].key, updaters[1].key}, tables[1]: {updaters[2].key}}, handlers[0])
	expected := &monitor{
		key2Updaters:     key2Updaters,
		updater2handlers: map[string][]handlerKey{updaters[0].key: {handlers[1]}, updaters[1].key: {handlers[1]}, updaters[2].key: {handlers[1]}},
	}
	assertEqualMonitors(t, expected, m)
}

// adding function to test monitor method of handler type (importent note:we are testing here "monitor" with lower-case m!")

func initHandlerSimple(t *testing.T) (handler *Handler, tctx context.Context, cli *clientv3.Client, cancel context.CancelFunc) {
	const ETCD_LOCALHOST = "localhost:2379"
	etcdServers := strings.Split(ETCD_LOCALHOST, ",")
	cli, err := NewEtcdClient(etcdServers)
	if err != nil {
		t.Error(err.Error())
	}
	db, _ := NewDatabaseEtcd(cli)
	tctx, cancel = context.WithCancel(context.Background())
	handler = NewHandler(tctx, db, cli)
	return
}

func addMonitorToHandler(handler *Handler, DatabaseName string, jsonValue string, monitorCondRequests_key_value_pairs []struct {
	key     string
	columns []string
}, LastTxnID *string, notificationType ovsjson.UpdateNotificationType) error {
	errStr := ""
	if DatabaseName == "" {
		errStr += errStr + " DatabaseName is empty."
	}
	n_pairs := len(monitorCondRequests_key_value_pairs)
	if n_pairs == 0 {
		errStr += errStr + " monitorCondRequests_key_value_pairs is empty"
	}
	if errStr != "" {
		errStr = "error :" + errStr
		return errors.New(errStr)
	}
	monitorCondRequestMap := make(map[string]ovsjson.MonitorCondRequest, n_pairs)
	for _, element := range monitorCondRequests_key_value_pairs {
		monitorCondRequestMap[element.key] = ovsjson.MonitorCondRequest{Columns: element.columns}
	}
	var params []interface{} = make([]interface{}, 4)
	params[0] = DatabaseName
	params[1] = jsonValue
	params[2] = monitorCondRequestMap
	params[3] = LastTxnID
	handler.monitor(params, notificationType)
	return nil
}

func TestHandlerAddMonitor(t *testing.T) {
	handler, _, _, cancel := initHandlerSimple(t)
	defer cancel()
	err := addMonitorToHandler(
		handler, "ovsdb/nb", "json-value-string", []struct {
			key     string
			columns []string
		}{
			struct {
				key     string
				columns []string
			}{"client-a", []string{"a1", "a2", "a3"}},
			struct {
				key     string
				columns []string
			}{"client-b", []string{"b1", "b2", "b3"}}}, nil, 1)
	if err != nil {
		t.Error(err.Error())
	}
}
