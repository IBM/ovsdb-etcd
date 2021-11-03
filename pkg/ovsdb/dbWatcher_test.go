package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
)

func TestDBWatcher(t *testing.T) {
	dbName := "test"
	key := common.GenerateDataKey(dbName, "table1")
	value := "test/value"
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer cli.Close()
	tcl := testCacheListener{expectedValue: value, expectedKey: key.String(), t: t}

	dbw := CreateDBWatcher(dbName, cli, 4367)
	dbw.cacheListener = &tcl
	dbw.start()
	cli.Put(context.Background(), key.String(), value)
	for i := 0; i < 5; i++ {
		if tcl.cacheUpdated {
			break
		}
		time.Sleep(time.Duration(1*i) * time.Second)
	}
	assert.True(t, tcl.cacheUpdated)
	cli.Delete(context.Background(), dbName, clientv3.WithFromKey())
}

func TestMonitorWatcher(t *testing.T) {
	const (
		dbName    = "OVN_Northbound"
		tableName = "NB_Global"
		col1      = "name"
		col2      = "ipsec"
		monid     = "monid"
	)
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer cli.Close()
	_, err = cli.Delete(context.Background(), common.GetPrefix(), clientv3.WithFromKey())
	assert.Nil(t, err)
	dbs, err := NewDatabaseEtcd(cli, ModStandalone, log)
	assert.Nil(t, err)
	db := dbs.(*DatabaseEtcd)
	err = db.AddSchema(path.Join("../../schemas", "_server.ovsschema"))
	assert.Nil(t, err)
	err = db.AddSchema(path.Join("../../schemas", "ovn-nb.ovsschema"))
	assert.Nil(t, err)
	ctx := context.Background()
	handler := NewHandler(ctx, db, cli, log)

	key := common.GenerateDataKey(dbName, tableName)
	row := map[string]interface{}{}
	row[col1] = "myName"
	row[col2] = true
	row[libovsdb.ColUuid] = libovsdb.UUID{GoUUID: key.UUID}
	row[libovsdb.ColVersion] = libovsdb.UUID{GoUUID: common.GenerateUUID()}
	value, err := json.Marshal(row)
	assert.Nil(t, err)
	ru := ovsjson.RowUpdate{New: &map[string]interface{}{col1: "myName", col2: true}}
	notifyMessage := []interface{}{[]interface{}{monid, dbName}, ovsjson.TableUpdates{tableName: ovsjson.TableUpdate{key.UUID: ru}}}
	wg := sync.WaitGroup{}
	wg.Add(1)
	jrpcServerMock := jrpcServerMock{
		expMethod:  Update,
		expMessage: notifyMessage,
		wg:         &wg,
		t:          t,
	}
	handler.SetConnection(&jrpcServerMock, nil)
	msg := fmt.Sprintf(`["%s",["%s","%s"],{"%s":[{"columns":["%s","%s"]}]}]`, dbName, monid, dbName, tableName, col1, col2)
	var params []interface{}
	err = json.Unmarshal([]byte(msg), &params)
	assert.Nil(t, err)
	jsonValueString := jsonValueToString(params[1])
	_, err = handler.addMonitor(params, ovsjson.Update)
	assert.Nil(t, err)
	handler.startNotifier(jsonValueString)
	cli.Put(ctx, key.String(), string(value))
	wg.Wait()
	// remove the monitor and validate that it was removed from the dbWatcher
	handler.removeMonitor(params[1], false)
	dbw, err := dbs.GetDBWatcher(dbName)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(dbw.monitorListeners))
	_, err = cli.Delete(ctx, common.GetPrefix(), clientv3.WithFromKey())
	assert.Nil(t, err)
}

type testCacheListener struct {
	t             *testing.T
	expectedKey   string
	expectedValue string
	cacheUpdated  bool
}

func (tcl *testCacheListener) updateCache(events []*clientv3.Event) {
	for _, event := range events {
		if string(event.Kv.Key) == tcl.expectedKey && string(event.Kv.Value) == tcl.expectedValue {
			tcl.cacheUpdated = true
			return
		}
	}
}
