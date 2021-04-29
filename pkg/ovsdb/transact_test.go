/* +build etcd */

package ovsdb

import (
	"context"
	"testing"

	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

const (
	DBNAME1 = "db1"
	TABLE1  = "table1"
)

func testEtcdNewCli() (*clientv3.Client, error) {
	endpoints := []string{"http://127.0.0.1:2379"}
	return NewEtcdClient(endpoints)
}

func testEtcdCleanup(t *testing.T, dbname, table string) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	prefix := makePrefix(dbname, table)
	_, err = cli.Delete(ctx, prefix, clientv3.WithPrefix())
	assert.Nil(t, err)
}

func testEtcdCleanupComment(t *testing.T, dbname string) {
	testEtcdCleanup(t, dbname, "_comment")
}

func testMergeKvs(kvs []*mvccpb.KeyValue, table string) (*map[string]interface{}, error) {
	dump := &map[string]interface{}{}
	for _, x := range kvs {
		kv, err := NewKeyValue(x)
		if err != nil {
			return nil, err
		}
		if kv.Table != table {
			continue
		}
		for k, v := range kv.Value {
			if k == COL_UUID || k == COL_VERSION {
				continue
			}
			(*dump)[k] = v
		}
	}
	return dump, nil
}

func testEtcdDump(t *testing.T, dbname, table string) map[string]interface{} {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	prefix := makePrefix(dbname, table)
	res, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	dump, err := testMergeKvs(res.Kvs, table)
	assert.Nil(t, err)
	return *dump
}

func testEtcdPut(t *testing.T, dbname, table, k string, v interface{}) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	uuid := guuid.New().String() /* generate RFC4122 UUID */
	key := makeKey(dbname, table, uuid)
	row := &map[string]interface{}{
		k: v,
	}
	setRowUUID(row, uuid)
	val, err := makeValue(row)
	assert.Nil(t, err)
	_, err = cli.Put(ctx, key, val)
	assert.Nil(t, err)
}

func testTransact(t *testing.T, req *libovsdb.Transact) *libovsdb.TransactResponse {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer cli.Close()
	txn := NewTransaction(cli, req)
	txn.Commit()
	return &txn.response
}

func TestTransactInsert(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: TABLE1,
				Row: map[string]interface{}{
					"key1": "val1",
				},
			},
		},
	}
	testEtcdCleanup(t, DBNAME1, TABLE1)
	resp := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, DBNAME1, TABLE1)
	assert.Equal(t, "val1", dump["key1"])
}

func TestTransactSelect(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op:    OP_SELECT,
				Table: TABLE1,
			},
		},
	}
	testEtcdCleanup(t, DBNAME1, TABLE1)
	testEtcdPut(t, DBNAME1, TABLE1, "key1", "val1")
	resp := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, DBNAME1, TABLE1)
	assert.Equal(t, "val1", dump["key1"])
}

func TestTransactUpdate(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: TABLE1,
				Row: map[string]interface{}{
					"key1": "val1",
				},
			},
			{
				Op:    OP_UPDATE,
				Table: TABLE1,
				Row: map[string]interface{}{
					"key1": "val2",
				},
			},
		},
	}
	testEtcdCleanup(t, DBNAME1, TABLE1)
	resp := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, DBNAME1, TABLE1)
	assert.Equal(t, "val2", dump["key1"])
}

func TestTransactMutate(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op:    OP_INSERT,
				Table: TABLE1,
				Row: map[string]interface{}{
					"key1": float64(1),
				},
			},
			{
				Op:    OP_MUTATE,
				Table: TABLE1,
				Mutations: []interface{}{
					[]interface{}{
						"key1",
						"+=",
						float64(1),
					},
				},
			},
		},
	}
	testEtcdCleanup(t, DBNAME1, TABLE1)
	resp := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, DBNAME1, TABLE1)
	assert.Equal(t, float64(2), dump["key1"])
}

func TestTransactDelete(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op:    OP_DELETE,
				Table: TABLE1,
			},
		},
	}
	testEtcdCleanup(t, DBNAME1, TABLE1)
	testEtcdPut(t, DBNAME1, TABLE1, "key1", "val1")
	resp := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
	dump := testEtcdDump(t, DBNAME1, TABLE1)
	_, ok := dump["key1"]
	assert.False(t, ok)
}

func TestTransactWait(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op: OP_WAIT,
			},
		},
	}
	resp := testTransact(t, req)
	assert.True(t, "" != resp.Error)
}

func TestTransactCommit(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op:      OP_COMMIT,
				Durable: true,
			},
		},
	}
	resp := testTransact(t, req)
	assert.True(t, "" != resp.Error)
}

func TestTransactAbort(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op: OP_ABORT,
			},
		},
	}
	resp := testTransact(t, req)
	assert.True(t, "" != resp.Error)
}

func TestTransactComment(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: DBNAME1,
		Operations: []libovsdb.Operation{
			{
				Op:      OP_COMMENT,
				Comment: "ovs-vsctl add-br br0",
			},
		},
	}
	testEtcdCleanupComment(t, DBNAME1)
	resp := testTransact(t, req)
	assert.Equal(t, "", resp.Error)
}

func TestTransactAssert(t *testing.T) {
}
