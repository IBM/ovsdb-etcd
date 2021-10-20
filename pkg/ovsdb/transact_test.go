package ovsdb

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/google/uuid"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

func TestMain(m *testing.M) {
	fs := flag.NewFlagSet("fs", flag.PanicOnError)
	klog.InitFlags(fs)
	err := fs.Set("v", "10")
	if err != nil {
		klog.Errorf("flags set %v", err)
		os.Exit(1)
	}
	flag.Parse()
	testSchemaSimple.AddInternalsColumns()
	testSchemaMutable.AddInternalsColumns()
	testSchemaSet.AddInternalsColumns()
	testSchemaUUID.AddInternalsColumns()
	testSchemaMap.AddInternalsColumns()
	testSchemaAtomic.AddInternalsColumns()
	testSchemaEnum.AddInternalsColumns()
	testSchemaIndex.AddInternalsColumns()
	testSchemaGC.AddInternalsColumns()
	common.SetPrefix("ovsdb/nb")
	os.Exit(m.Run())
}

var testSchemaSimple = &libovsdb.DatabaseSchema{
	Name:    "simple",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"key1": {
					Type: libovsdb.TypeString,
				},
				"key2": {
					Type: libovsdb.TypeInteger,
				},
			},
		},
	},
}

var testSchemaAtomic = &libovsdb.DatabaseSchema{
	Name:    "atomic",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"string": {
					Type: libovsdb.TypeString,
				},
				"integer": {
					Type: libovsdb.TypeInteger,
				},
				"real": {
					Type: libovsdb.TypeReal,
				},
				"boolean": {
					Type: libovsdb.TypeBoolean,
				},
				"uuid": {
					Type: libovsdb.TypeUUID,
				},
			},
		},
	},
}

var testSchemaMutable = &libovsdb.DatabaseSchema{
	Name:    "mutable",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"mutable": {
					Type: libovsdb.TypeInteger,
				},
				"unmutable": {
					Type:    libovsdb.TypeInteger,
					Mutable: new(bool),
				},
			},
		},
	},
}

var testSchemaEnum = &libovsdb.DatabaseSchema{
	Name:    "enum",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"color": {
					Type: libovsdb.TypeEnum,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
							Enum: &libovsdb.OvsSet{GoSet: []interface{}{"red", "green", "blue"}},
						},
					},
				},
				"animal": {
					Type: libovsdb.TypeEnum,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
							Enum: &libovsdb.OvsSet{GoSet: []interface{}{"dog", "cat", "mouse"}},
						},
					},
				},
			},
		},
	},
}

var testSchemaSet = &libovsdb.DatabaseSchema{
	Name:    "set",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"string": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Max: libovsdb.Unlimited,
						Min: 0,
					},
				},
				"integer": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeInteger,
						},
						Max: 3,
						Min: 0,
					},
				},
				"uuid": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
						},
						Max: libovsdb.Unlimited,
						Min: 0,
					},
				},
			},
		},
	},
}

var testSchemaMap = &libovsdb.DatabaseSchema{
	Name:    "map",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"string": {
					Type: libovsdb.TypeMap,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Min: 0,
						Max: libovsdb.Unlimited,
					},
				},
			},
		},
	},
}

var testSchemaUUID = &libovsdb.DatabaseSchema{
	Name:    "uuid",
	Version: "0.0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"uuid": {
					Type: libovsdb.TypeUUID,
				},
			},
		},
		"table2": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"map": {
					Type: libovsdb.TypeMap,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeString,
						},
						Value: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
						},
						Min: 0,
						Max: libovsdb.Unlimited,
					},
				},
				"set": {
					Type: libovsdb.TypeSet,
					TypeObj: &libovsdb.ColumnType{
						Key: &libovsdb.BaseType{
							Type: libovsdb.TypeUUID,
						},
						Min: 0,
						Max: libovsdb.Unlimited,
					},
				},
			},
		},
	},
}

var testSchemaIndex = &libovsdb.DatabaseSchema{
	Name:    "index",
	Version: "0.0.0",
	Tables: map[string]libovsdb.TableSchema{
		"table1": {
			Columns: map[string]*libovsdb.ColumnSchema{
				"name": {
					Type: libovsdb.TypeString,
				},
				"col1": {
					Type: libovsdb.TypeInteger,
				},
				"col2": {
					Type: libovsdb.TypeString,
				},
				"col3": {
					Type: libovsdb.TypeString,
				},
			},
			Indexes: [][]string{{"name"}, {"col1", "col2"}},
		},
	},
}

func testEtcdNewCli() (*clientv3.Client, error) {
	endpoints := []string{"http://127.0.0.1:2379"}
	return NewEtcdClient(context.Background(), endpoints, -1*time.Second, -1*time.Second)
}

func testEtcdCleanup(t *testing.T) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	_, err = cli.Delete(ctx, "", clientv3.WithPrefix())
	assert.Nil(t, err)
}

func testEtcdPut(t *testing.T, dbname, table string, uuid string, row map[string]interface{}) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	if uuid == "" {
		uuid = common.GenerateUUID()
	}
	key := common.NewDataKey(dbname, table, uuid)
	setRowUUID(&row, key.UUID)
	val, err := makeValue(&row)
	assert.Nil(t, err)
	_, err = cli.Put(ctx, key.String(), val)
	assert.Nil(t, err)
}

func testEtcdGetComment(t *testing.T, comment string) {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	ctx := context.TODO()
	key := common.NewCommentTableKey()
	response, err := cli.Get(ctx, key.String(), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	assert.Nil(t, err)
	for _, kv := range response.Kvs {
		if string(kv.Value) == comment {
			return
		}
	}
	assert.True(t, false, "Comment not found")
}

func testTransact(t *testing.T, req *libovsdb.Transact, schema *libovsdb.DatabaseSchema, expCacheElements int) *libovsdb.TransactResponse {
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer func() {
		err := cli.Close()
		assert.Nil(t, err)
	}()
	cache := cache{}
	err = cache.addDatabaseCache(schema, cli, klogr.New())
	assert.Nil(t, err)
	dbCache := cache.getDBCache(schema.Name)
	if expCacheElements > -1 {
		var elements int
		for i := 0; i < 5; i++ {
			elements = (&cache).size()
			if elements == expCacheElements {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		assert.Equal(t, expCacheElements, elements)
	}
	txn, err := NewTransaction(context.Background(), cli, req, dbCache, schema, klogr.New())
	assert.Nil(t, err)
	// TODO check error
	txn.Commit()
	//assert.Nil(t, err)
	return &txn.response
}

func TestTransactInsertSimple(t *testing.T) {
	table := "table1"
	dbName := "simple"

	row := map[string]interface{}{
		"key1": "val1",
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 1, 0, "")

	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
}

func testTransactInsertSimpleScale(t *testing.T, n int) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	op := libovsdb.Operation{
		Op:    libovsdb.OperationInsert,
		Table: &table,
		Row:   &row,
	}

	req := &libovsdb.Transact{
		DBName:     dbName,
		Operations: []libovsdb.Operation{},
	}
	for i := 0; i < n; i++ {
		req.Operations = append(req.Operations, op)
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	assert.Equal(t, n, len(resp.Result))
	for i := 0; i < n; i++ {
		validateInsertResult(t, resp, n, i, "")
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, n)
	validateSelectResult(t, resp, 1, 0, n)
}

func TestTransactInsertSimpleScale10(t *testing.T) {
	testTransactInsertSimpleScale(t, 10)
}

func TestTransactInsertSimpleScale100(t *testing.T) {
	testTransactInsertSimpleScale(t, 100)
}

/*
XXX: If run with default setting then it will fail with message:
XXX:
XXX:   etcdserver: too many operations in txn request
XXX:
XXX: To enable the test to pass:
XXX: 1. use etcdTrx version 3.4.x and above
XXX: 2. configure in /etc/default/etcdTrx:
XXX:
XXX:   ETCD_MAX_TXN_OPS=100000
XXX:   ETCD_MAX_REQUEST_BYTES=100000000
XXX:
func TestTransactInsertSimpleScale1000(t *testing.T) {
	testTransactInsertSimpleScale(t, 1000)
}
*/

func TestTransactInsertSimpleWithUUID(t *testing.T) {
	table := "table1"
	dbName := "simple"
	uuid := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	row := map[string]interface{}{
		"key1": "val1",
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
				UUID:  &uuid,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 1, 0, uuid.GoUUID)

	// select check
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
}

func validateSelectResult(t *testing.T, resp *libovsdb.TransactResponse, results, checkedOperation, expectedRows int) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].Details)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.NotNil(t, resp.Result[checkedOperation].Rows)
	assert.Equal(t, expectedRows, len(*resp.Result[checkedOperation].Rows))
}

func validateEmptyResult(t *testing.T, resp *libovsdb.TransactResponse, results int, checkedOperation int) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].Details)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.Nil(t, resp.Result[checkedOperation].Rows)
}

func validateOperationError(t *testing.T, resp *libovsdb.TransactResponse, results int, checkedOperation int, errorMsg string) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.NotNil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.Nil(t, resp.Result[checkedOperation].Rows)
	if errorMsg != "" {
		assert.Equal(t, errorMsg, *resp.Result[0].Error)
	}
}

func validateUpdateResult(t *testing.T, resp *libovsdb.TransactResponse, results, checkedOperation, expectedCount int) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.NotNil(t, resp.Result[checkedOperation].Count)
	assert.Equal(t, expectedCount, *resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	assert.Nil(t, resp.Result[checkedOperation].Details)
	assert.Nil(t, resp.Result[checkedOperation].UUID)
	assert.Nil(t, resp.Result[checkedOperation].Rows)
}

func validateInsertResult(t *testing.T, resp *libovsdb.TransactResponse, results int, checkedOperation int, expectedUUID string) {
	assert.Nil(t, resp.Error)
	assert.Equal(t, results, len(resp.Result))
	assert.Nil(t, resp.Result[checkedOperation].Count)
	assert.Nil(t, resp.Result[checkedOperation].Error)
	if resp.Result[checkedOperation].Error != nil {
		log.Info("validateInsertResult", "error", resp.Result[checkedOperation].Error)
	}
	assert.Nil(t, resp.Result[checkedOperation].Details)
	if resp.Result[checkedOperation].Details != nil {
		log.Info("validateInsertResult", "details", resp.Result[checkedOperation].Details)
	}
	assert.Nil(t, resp.Result[checkedOperation].Rows)
	if resp.Result[checkedOperation].Error == nil {
		assert.NotNil(t, resp.Result[checkedOperation].UUID)
		if expectedUUID != "" {
			assert.Equal(t, expectedUUID, resp.Result[checkedOperation].UUID.GoUUID)
		}
	}

}

func TestTransactInsertSimpleWithUUIDName(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuidName := "myuuid"
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, -1)
	validateInsertResult(t, resp, 1, 0, "")
	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
}

func TestTransactInsertSimpleWithUUIDNameDupError(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuidName := "myuuid"
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row,
				UUIDName: &uuidName,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 2, 0, "")
	assert.Nil(t, resp.Result[1].Count)
	assert.NotNil(t, resp.Result[1].Error)
	assert.Equal(t, ErrDuplicateUUIDName, *resp.Result[1].Error)
	assert.Nil(t, resp.Result[1].Rows)
}

/*
func TestTransactInsertSimpleWithUUIDDupError(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuid := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
				UUID:  &uuid,
			},
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
				UUID:  &uuid,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 2, 0, "")
	assert.Nil(t, resp.Result[1].Count)
	assert.NotNil(t, resp.Result[1].Error)
	assert.Equal(t, ErrDuplicateUUID, *resp.Result[1].Error)
	assert.Nil(t, resp.Result[1].Rows)
}

func TestTransactInsertSimpleWithUUIDDupError2(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row := map[string]interface{}{
		"key1": "val1",
	}
	uuid := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
				UUID:  &uuid,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, 0)
	validateInsertResult(t, resp, 1, 0, uuid.GoUUID)

	resp = testTransact(t, req, testSchemaSimple, 1)
	assert.Nil(t, resp.Result[0].Count)
	assert.NotNil(t, resp.Result[0].Error)
	assert.Equal(t, ErrDuplicateUUID, *resp.Result[0].Error)
	assert.Nil(t, resp.Result[0].Rows)

}
*/
func TestTransactAtomicInsertNamedUUID(t *testing.T) {
	dbName := "atomic"
	table := "table1"
	uuidName1 := libovsdb.UUID{GoUUID: "myuuid1"}
	uuid1 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}

	row1 := map[string]interface{}{
		"string": "val1",
	}
	row2 := map[string]interface{}{
		"uuid": uuidName1,
	}

	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row1,
				UUID:     &uuid1,
				UUIDName: &uuidName1.GoUUID,
			},
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}

	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaAtomic, 0)
	validateInsertResult(t, resp, 2, 0, uuid1.GoUUID)
	validateUpdateResult(t, resp, 2, 1, 1)

	//validate new row
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, uuid1}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaAtomic, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	err := testSchemaAtomic.Unmarshal(table, &irow)
	assert.Nil(t, err)
	assert.Equal(t, uuid1, irow["uuid"])
}

func TestTransactAtomicInsertUUID(t *testing.T) {
	table := "table1"
	uuid1 := libovsdb.UUID{GoUUID: "00000000-0000-0000-0000-000000000001"}
	uuid1buf, err := json.Marshal(uuid1)
	assert.Nil(t, err)
	var uuid1array []interface{}
	err = json.Unmarshal(uuid1buf, &uuid1array)
	assert.Nil(t, err)

	row1 := map[string]interface{}{
		"string": "val1",
	}

	row2 := map[string]interface{}{
		"string": "val2",
	}

	where2 := []interface{}{
		[]interface{}{"_uuid", "==", uuid1array},
	}

	req1 := &libovsdb.Transact{
		DBName: "atomic",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
				UUID:  &uuid1,
			},
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
				Where: &where2,
			},
		},
	}

	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaAtomic, -1)
	assert.Nil(t, resp.Error)

	// FIXME
	//dump := testEtcdDump(t, "atomic", "table1")
	//assert.Equal(t, []interface{}{"uuid", uuid1.GoUUID}, dump["_uuid"])
}

func TestTransactInsertEnumOk(t *testing.T) {
	dbName := "enum"
	table := "table1"
	row := map[string]interface{}{
		"color": "red",
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaEnum, 0)
	validateInsertResult(t, resp, 1, 0, "")

	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaEnum, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "red", row["color"])
}

func TestTransactInsertEnumError(t *testing.T) {
	table := "table1"
	row := map[string]interface{}{
		"animal": "red",
	}
	req := &libovsdb.Transact{
		DBName: "enum",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaEnum, -1)
	assert.NotEqual(t, "", resp.Error)
}

func TestTransactInsertSetOk(t *testing.T) {
	dbName := "set"
	table := "table1"
	dbSchemas := testSchemaSet
	set := libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}}
	row := map[string]interface{}{"string": set}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, dbSchemas, 0)
	validateInsertResult(t, resp, 1, 0, "")

	//validate new row
	uuid := resp.Result[0].UUID
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, dbSchemas, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	err := dbSchemas.Unmarshal(table, &row)
	assert.Nil(t, err)
	assert.Equal(t, set, row["string"])
}

func TestTransactInsertSetError(t *testing.T) {
	dbName := "set"
	table := "table1"
	dbSchemas := testSchemaSet
	row := map[string]interface{}{
		"string": libovsdb.OvsSet{GoSet: []interface{}{1, 2, 3}},
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, dbSchemas, 0)
	validateOperationError(t, resp, 1, 0, ErrConstraintViolation)
}

func TestTransactUpdateSimple(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row1 := map[string]interface{}{
		"key1": "val1",
	}
	row2 := map[string]interface{}{
		"key1": "val2",
	}
	row3 := map[string]interface{}{
		"key1": "val3",
	}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{Op: libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
			{Op: libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row3,
			},
		},
	}
	testEtcdCleanup(t)
	// insert a row
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 3, 0, "")
	validateUpdateResult(t, resp, 3, 1, 1)
	validateUpdateResult(t, resp, 3, 2, 1)

	// check the updated value
	req3 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req3, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val3", row["key1"])
}

func TestTransactUpdateMT(t *testing.T) {
	table := "table1"
	dbName := "simple"
	n := 2
	row := map[string]interface{}{
		"key1": "val1",
	}
	row2 := map[string]interface{}{
		"key1": "val2",
	}
	row3 := map[string]interface{}{
		"key1": "val3",
	}

	uuids := make([]libovsdb.UUID, n)
	for i := 0; i < n; i++ {
		uuids[i] = libovsdb.UUID{GoUUID: common.GenerateUUID()}
	}

	ops := make([]libovsdb.Operation, n)
	for i := 0; i < n; i++ {
		ops[i] = libovsdb.Operation{
			Op:    libovsdb.OperationInsert,
			Table: &table,
			Row:   &row,
			UUID:  &uuids[i],
		}
	}

	req := &libovsdb.Transact{
		DBName:     dbName,
		Operations: ops,
	}
	testEtcdCleanup(t)
	// insert a row
	resp := testTransact(t, req, testSchemaSimple, 0)
	for i := 0; i < n; i++ {
		validateInsertResult(t, resp, n, i, uuids[i].GoUUID)
	}

	for i := 0; i < n; i++ {
		ops[i] = libovsdb.Operation{
			Op:    libovsdb.OperationUpdate,
			Table: &table,
			Row:   &row2,
			Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, uuids[i]}},
		}
	}
	ops = append(ops, libovsdb.Operation{Op: libovsdb.OperationUpdate, Table: &table, Row: &row3})

	req = &libovsdb.Transact{
		DBName:     dbName,
		Operations: ops,
	}
	resp = testTransact(t, req, testSchemaSimple, n)
	for i := 0; i < n; i++ {
		validateUpdateResult(t, resp, n+1, i, 1)
	}
	validateUpdateResult(t, resp, n+1, n, n)

	// check the updated value
	ops = make([]libovsdb.Operation, n)
	for i := 0; i < n; i++ {
		ops[i] = libovsdb.Operation{
			Op:      libovsdb.OperationSelect,
			Table:   &table,
			Where:   &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, uuids[i]}},
			Columns: &[]string{"key1", "key2"},
		}
	}
	req = &libovsdb.Transact{
		DBName:     dbName,
		Operations: ops,
	}
	resp = testTransact(t, req, testSchemaSimple, n)
	for i := 0; i < n; i++ {
		validateSelectResult(t, resp, n, i, 1)
		row := (*resp.Result[0].Rows)[0]
		assert.Equal(t, "val3", row["key1"])
	}
}

func TestTransactUpdateMapOk(t *testing.T) {
	table := "table1"
	dbName := "map"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
			"key1": "value1a",
			"key2": "value2",
		}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1b"}},
	}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaMap, 0)
	assert.Nil(t, resp.Error)
	uuid := resp.Result[0].UUID

	// update the row
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req2, testSchemaMap, 1)
	validateUpdateResult(t, resp, 1, 0, 1)

	// check the updated value
	req3 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid}},
			},
		},
	}
	resp = testTransact(t, req3, testSchemaMap, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	err := testSchemaMap.Unmarshal(table, &irow)
	assert.Nil(t, err)
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1b", "key2": "value2"}}, irow["string"])
}

/*
func TestTransactUpdateMap2Txn(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
			"key1": "value1a",
			"key2": "value2",
		}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1b"}},
	}
	req1 := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	testEtcdCleanup(t)
	/ * txn 1 * /
	resp, txn := testTransact(t, req1, testSchemaMap, -1)
	assert.Nil(t, resp.Error)
	dump := testTransactDump(t, txn, "map", "table1")
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
		"key1": "value1a",
		"key2": "value2",
	}}, dump["string"])

	/ * txn 2 * /
	resp, txn = testTransact(t, req2, testSchemaMap, -1)
	assert.Nil(t, resp.Error)
	dump = testTransactDump(t, txn, "map", "table1")
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
		"key1": "value1b",
		"key2": "value2",
	}}, dump["string"])
}
*/

/*
func TestTransactUpdateMapError(t *testing.T) {
	table := "table1"
	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}},
	}
	row2 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": int(2) / * error * /}},
	}
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	testEtcdCleanup(t)
	resp, _ := testTransact(t, req, testSchemaMap, -1)
	assert.NotEqual(t, "", resp.Error)
}
*/

func TestTransactUpdateUnmutableError(t *testing.T) {
	table := "table1"
	dbName := "mutable"
	row1 := map[string]interface{}{
		"mutable":   1,
		"unmutable": 1,
	}
	row2 := map[string]interface{}{
		"mutable":   2,
		"unmutable": 2,
	}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaMutable, 0)
	assert.Nil(t, resp.Error)
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationUpdate,
				Table: &table,
				Row:   &row2,
			},
		},
	}
	resp = testTransact(t, req2, testSchemaMutable, 1)
	validateOperationError(t, resp, 1, 0, ErrConstraintViolation)
}

func TestTransactMutateSimple(t *testing.T) {
	table := "table1"
	dbName := "simple"
	row1 := map[string]interface{}{"key2": 1}
	mutations := []interface{}{
		[]interface{}{"key2", "+=", 1},
	}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 4, 0, "")
	validateUpdateResult(t, resp, 4, 1, 1)
	validateUpdateResult(t, resp, 4, 2, 1)
	validateUpdateResult(t, resp, 4, 3, 1)

	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	assert.EqualValues(t, 4, row["key2"])
}

func TestTransactMutateSetUUID(t *testing.T) {
	table := "table2"
	dbName := "uuid"
	dbSchemas := testSchemaUUID
	uuid1 := "00000000-0000-0000-0000-000000000001"
	uuid2 := "00000000-0000-0000-0000-000000000002"
	uuidName := "rowdea9b92e_4b83_4fdc_b552_8ec4b9d3581f"
	goUUID := libovsdb.UUID{GoUUID: uuidName}
	table2row1 := map[string]interface{}{
		"set": libovsdb.OvsSet{GoSet: []interface{}{
			libovsdb.UUID{GoUUID: uuid1},
		}},
	}

	mutations := []interface{}{
		[]interface{}{
			"set",
			MtDelete,
			libovsdb.OvsSet{GoSet: []interface{}{
				libovsdb.UUID{GoUUID: uuid1},
			}},
		},
		[]interface{}{
			"set",
			MtInsert,
			libovsdb.OvsSet{GoSet: []interface{}{
				libovsdb.UUID{GoUUID: uuid2},
			}},
		},
	}

	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &table2row1,
				UUIDName: &uuidName,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
				Where:     &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, goUUID}},
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, dbSchemas, 0)
	validateInsertResult(t, resp, 2, 0, "")
	validateUpdateResult(t, resp, 2, 1, 1)

	req3 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req3, dbSchemas, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	err := dbSchemas.Unmarshal(table, &irow)
	assert.Nil(t, err)
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{libovsdb.UUID{GoUUID: uuid2}}}, irow["set"])
}

func TestTransactMutateMap(t *testing.T) {
	table := "table1"
	dbName := "map"
	dbSchemas := testSchemaMap

	row1 := map[string]interface{}{
		"string": libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
			"key1": "value1",
			"key2": "value2",
		}},
	}

	mutations := []interface{}{
		[]interface{}{
			"string",
			MtInsert,
			libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
				"key2": "valueNew1",
				"key3": "value3",
			}},
		},
		[]interface{}{
			"string",
			MtInsert,
			libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
				"key2": "valueNew2",
				"key4": "value4",
			}},
		},
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaMap, 0)
	validateInsertResult(t, resp, 2, 0, "")
	validateUpdateResult(t, resp, 2, 1, 1)

	reqS := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, reqS, dbSchemas, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	err := dbSchemas.Unmarshal(table, &irow)
	assert.Nil(t, err)
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	},
	}, irow["string"])

	mutations = []interface{}{
		[]interface{}{
			"string",
			MtDelete,
			libovsdb.OvsMap{GoMap: map[interface{}]interface{}{
				"key1": "value2", // different value, should not be removed
				"key2": "value2",
			}},
		},
		[]interface{}{
			"string",
			MtDelete,
			libovsdb.OvsSet{GoSet: []interface{}{"key3", "key4", "key5"}},
		},
	}
	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations,
			},
		},
	}
	resp = testTransact(t, req, dbSchemas, 1)
	validateUpdateResult(t, resp, 1, 0, 1)
	resp = testTransact(t, reqS, dbSchemas, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	irow = row
	err = dbSchemas.Unmarshal(table, &irow)
	assert.Nil(t, err)
	assert.Equal(t, libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "value1"}}, irow["string"])
}

func TestTransactInsertTwoNamedUUID(t *testing.T) {
	table := "table1"
	dbName := "uuid"
	dbSchemas := testSchemaUUID

	uuidName := "rowdea9b92e_4b83_4fdc_b552_8ec4b9d3581f"
	uuid1 := "00000000-0000-0000-0000-000000000001"
	goUUID1 := libovsdb.UUID{GoUUID: uuid1}
	row1 := map[string]interface{}{"uuid": libovsdb.UUID{GoUUID: uuidName}}
	row2 := map[string]interface{}{"uuid": libovsdb.UUID{GoUUID: uuid1}}
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
				UUID:  &goUUID1,
			},
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table,
				Row:      &row2,
				UUIDName: &uuidName,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, dbSchemas, 0)
	validateInsertResult(t, resp, 2, 0, uuid1)
	validateInsertResult(t, resp, 2, 1, "")
	uuid := resp.Result[1].UUID.GoUUID

	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, goUUID1}},
			},
		},
	}
	resp = testTransact(t, req2, dbSchemas, 2)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	err := dbSchemas.Unmarshal(table, &irow)
	assert.Nil(t, err)
	assert.Equal(t, libovsdb.UUID{GoUUID: uuid}, irow["uuid"])
}

func TestTransactMutateSet(t *testing.T) {
	dbName := "set"
	table := "table1"
	dbSchemas := testSchemaSet
	row1 := map[string]interface{}{
		"string":  libovsdb.OvsSet{GoSet: []interface{}{"a", "b", "c"}},
		"integer": libovsdb.OvsSet{GoSet: []interface{}{1, 2, 3}},
	}
	mutations1 := []interface{}{
		[]interface{}{
			"string",
			MtInsert,
			libovsdb.OvsSet{GoSet: []interface{}{"c", "d"}},
		},
	}
	mutations2 := []interface{}{
		[]interface{}{
			"integer",
			MtProduct,
			2,
		},
	}
	mutations3 := []interface{}{
		[]interface{}{
			"string",
			MtDelete,
			libovsdb.OvsSet{GoSet: []interface{}{"b", "d", "e"}},
		},
		[]interface{}{
			"integer",
			MtDelete,
			libovsdb.OvsSet{GoSet: []interface{}{4, 7}},
		},
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations1,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations2,
			},
			{
				Op:        libovsdb.OperationMutate,
				Table:     &table,
				Mutations: &mutations3,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, dbSchemas, 0)
	validateInsertResult(t, resp, 4, 0, "")
	validateUpdateResult(t, resp, 4, 1, 1)
	validateUpdateResult(t, resp, 4, 2, 1)
	validateUpdateResult(t, resp, 4, 3, 1)

	// get the value
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, req2, dbSchemas, 1)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	irow := map[string]interface{}(row)
	err := dbSchemas.Unmarshal(table, &irow)
	assert.Nil(t, err)
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{"a", "c"}}, irow["string"])
	assert.Equal(t, libovsdb.OvsSet{GoSet: []interface{}{float64(2), float64(6)}}, irow["integer"])
}

func TestTransactionSelectByIndex(t *testing.T) {
	table := "table1"
	dbName := "index"

	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{"name", FuncEQ, "nameValue"}},
			},
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{"col1", FuncEQ, "col1Value"}, []interface{}{"col2", FuncEQ, "col2Value"}},
			},
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{"col1", FuncEQ, "col1Value"}, []interface{}{"col3", FuncEQ, "col3Value"}},
			},
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{"col1", FuncEQ, "col1Value"}},
			},
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{"col1", FuncEQ, "col1Value"}, []interface{}{"name", FuncEQ, "nameValue"}},
			},
		},
	}

	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	trx, err := NewTransaction(context.Background(), cli, req, &databaseCache{}, testSchemaIndex, klogr.New())
	assert.Nil(t, err)
	ok, err := trx.isSpecificRowSelected(&req.Operations[0])
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = trx.isSpecificRowSelected(&req.Operations[1])
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = trx.isSpecificRowSelected(&req.Operations[2])
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = trx.isSpecificRowSelected(&req.Operations[3])
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = trx.isSpecificRowSelected(&req.Operations[4])
	assert.Nil(t, err)
	assert.True(t, ok)
}

func TestTransactionDeleteInsert(t *testing.T) {
	table := "table1"
	dbName := "index"

	row := map[string]interface{}{
		"name": "rowName",
		"col3": "val3",
	}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaIndex, 0)
	validateInsertResult(t, resp, 1, 0, "")
	oldUUID := resp.Result[0].UUID

	row["col3"] = "newVal"
	row["col2"] = "val2"

	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{"name", FuncEQ, "rowName"}},
			},
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	resp = testTransact(t, req, testSchemaIndex, 1)
	validateInsertResult(t, resp, 2, 1, "")
	newUUID := resp.Result[1].UUID
	assert.NotEqual(t, oldUUID.GoUUID, newUUID.GoUUID)

	selectReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	resp = testTransact(t, selectReq, testSchemaIndex, 1)
	retRow := (*resp.Result[0].Rows)[0]
	assert.Equal(t, "rowName", retRow["name"])
	assert.Equal(t, "newVal", retRow["col3"])

	// check update all lines not selected by index
	row["col2"] = "val3"
	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{"col3", FuncEQ, "newVal"}},
			},
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row,
			},
		},
	}
	resp = testTransact(t, req, testSchemaIndex, 1)
	validateInsertResult(t, resp, 2, 1, "")
	resp = testTransact(t, selectReq, testSchemaIndex, 1)
	retRow = (*resp.Result[0].Rows)[0]
	assert.Equal(t, "rowName", retRow["name"])
	assert.Equal(t, "newVal", retRow["col3"])
	assert.Equal(t, "val3", retRow["col2"])
}

func TestTransactDelete(t *testing.T) {
	table := "table1"
	dbName := "simple"
	uuid := common.GenerateUUID()
	goUUID := libovsdb.UUID{GoUUID: uuid}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationDelete,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, goUUID}},
			},
		},
	}
	testEtcdCleanup(t)
	testEtcdPut(t, dbName, table, "", map[string]interface{}{
		"key1": "val1",
		"key2": 3,
	})

	testEtcdPut(t, dbName, table, uuid, map[string]interface{}{
		"key3": "val1",
		"key4": 3,
	})
	resp := testTransact(t, req, testSchemaSimple, 2)
	validateUpdateResult(t, resp, 1, 0, 1)
	// check select
	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, goUUID}},
			},
		},
	}
	resp = testTransact(t, req, testSchemaSimple, 1)
	validateSelectResult(t, resp, 1, 0, 0)
}

func TestTransactWaitSimpleEQ(t *testing.T) {
	table := "table1"
	dbName := "simple"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1a",
		},
	}
	until := FuncEQ
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

// FIXME insert + timeout
func TestTransactWaitSimpleEQColumnsNil(t *testing.T) {
	table := "table1"
	dbName := "simple"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	rows := []map[string]interface{}{
		{
			"key1": "val1a",
		},
	}
	until := FuncEQ
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

func TestTransactWaitSimpleEQRowsEmpty(t *testing.T) {
	table := "table1"
	dbName := "simple"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	var rows []map[string]interface{}
	columns := []string{"key1"}
	until := FuncEQ
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

// FIXME timeouts
func TestTransactWaitSimpleNE(t *testing.T) {
	table := "table1"
	dbName := "simple"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
		"key2": 1,
	}
	row2 := map[string]interface{}{
		"key1": "val1b",
		"key2": 1,
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1c",
		},
	}
	until := FuncNE
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row2,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 2, 0, "")
	validateInsertResult(t, resp, 2, 1, "")
	resp = testTransact(t, req2, testSchemaSimple, 2)
	validateEmptyResult(t, resp, 1, 0)
}

func TestTransactWaitSimpleEQError(t *testing.T) {
	table := "table1"
	dbName := "simple"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1b",
		},
	}
	until := FuncEQ
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	assert.Nil(t, resp.Error)
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateOperationError(t, resp, 1, 0, ErrTimeout)
}

func TestTransactWaitSimpleNEError(t *testing.T) {
	table := "table1"
	dbName := "simple"
	timeout := 0
	row1 := map[string]interface{}{
		"key1": "val1a",
	}
	columns := []string{"key1"}
	rows := []map[string]interface{}{
		{
			"key1": "val1a",
		},
	}
	until := FuncNE
	req1 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationInsert,
				Table:   &table,
				Row:     &row1,
				Timeout: &timeout,
			},
		},
	}
	req2 := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req1, testSchemaSimple, 0)
	validateInsertResult(t, resp, 1, 0, "")
	resp = testTransact(t, req2, testSchemaSimple, 1)
	validateOperationError(t, resp, 1, 0, ErrTimeout)
}

func testColumnDefault(t *testing.T, from interface{}) interface{} {
	buf, err := json.Marshal(from)
	assert.Nil(t, err)
	var to []interface{}
	err = json.Unmarshal(buf, &to)
	assert.Nil(t, err)
	return to
}

// FIXME
func TestTransactWaitMapEQ(t *testing.T) {
	table := "table1"
	timeout := 0
	actual1 := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"key1": "a,b,c"}}
	//expected1 := actual1
	row1 := map[string]interface{}{
		"string": testColumnDefault(t, actual1),
	}
	columns := []string{"string"}
	rows := []map[string]interface{}{row1}

	until := FuncEQ
	req := &libovsdb.Transact{
		DBName: "map",
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &table,
				Row:   &row1,
			},
			{
				Op:      libovsdb.OperationWait,
				Table:   &table,
				Rows:    &rows,
				Columns: &columns,
				Until:   &until,
				Timeout: &timeout,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaMap, -1)
	validateInsertResult(t, resp, 2, 0, "")
	//validateEmptyResult(t, resp, 2, 1)
}

func TestTransactCommit(t *testing.T) {
	durable := true
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationCommit,
				Durable: &durable,
			},
		},
	}
	resp := testTransact(t, req, testSchemaSimple, -1)
	validateOperationError(t, resp, 1, 0, ErrNotSupported)
}

func TestTransactAbort(t *testing.T) {
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op: libovsdb.OperationAbort,
			},
		},
	}
	resp := testTransact(t, req, testSchemaSimple, -1)
	validateOperationError(t, resp, 1, 0, ErrAborted)
}

func TestTransactSelect(t *testing.T) {
	table := "table1"
	dbName := "simple"
	uuid1 := common.GenerateUUID()
	goUUID1 := libovsdb.UUID{GoUUID: uuid1}
	var uuid2 string
	for true {
		uuid2 = common.GenerateUUID()
		if uuid2 != uuid1 {
			break
		}
	}
	var uuid3 string
	goUUID3 := libovsdb.UUID{GoUUID: uuid3}
	for true {
		uuid3 = common.GenerateUUID()
		if uuid3 != uuid1 {
			break
		}
	}

	simpleReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
		},
	}
	UUIDReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, goUUID1}},
			},
		},
	}
	noneUUIDReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEX, goUUID1}},
			},
		},
	}
	emptyReq := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, goUUID3}},
			},
		},
	}

	testEtcdCleanup(t)
	testEtcdPut(t, dbName, table, uuid2, map[string]interface{}{
		"key1": "val1",
		"key2": 3,
	})

	testEtcdPut(t, dbName, table, uuid1, map[string]interface{}{
		"key3": "val1",
		"key4": 3,
	})
	checkUUIDRow := func(row libovsdb.ResultRow) {
		assert.Equal(t, "val1", row["key3"])
		assert.EqualValues(t, 3, row["key4"])
	}
	checkNoneUUIDRow := func(row libovsdb.ResultRow) {
		assert.Equal(t, "val1", row["key1"])
		assert.EqualValues(t, 3, row["key2"])
	}

	resp := testTransact(t, simpleReq, testSchemaSimple, 2)
	validateSelectResult(t, resp, 1, 0, 2)
	for i := 0; i < 2; i++ {
		row := (*resp.Result[0].Rows)[i]
		iUUID, err := libovsdb.UnmarshalUUID(row[libovsdb.ColUuid])
		assert.Nil(t, err)
		goUUID, ok := iUUID.(libovsdb.UUID)
		assert.True(t, ok)
		if goUUID.GoUUID == uuid1 {
			checkUUIDRow(row)
		} else {
			checkNoneUUIDRow(row)
		}
	}
	// where [[_uuid == uuid1]]
	resp = testTransact(t, UUIDReq, testSchemaSimple, 2)
	validateSelectResult(t, resp, 1, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	checkUUIDRow(row)
	// where [[_uuid exclude uuid]]
	resp = testTransact(t, noneUUIDReq, testSchemaSimple, 2)
	validateSelectResult(t, resp, 1, 0, 1)
	row = (*resp.Result[0].Rows)[0]
	checkNoneUUIDRow(row)
	// where [[_uuid == uuid3]]
	resp = testTransact(t, emptyReq, testSchemaSimple, 2)
	validateSelectResult(t, resp, 1, 0, 0)
}

func TestTransactSelectAndComment(t *testing.T) {
	comment := "select test data"
	table := "table1"
	dbName := "simple"

	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table,
			},
			{
				Op:      libovsdb.OperationComment,
				Comment: &comment,
			},
		},
	}
	testEtcdCleanup(t)
	testEtcdPut(t, dbName, table, "", map[string]interface{}{
		"key1": "val1",
		"key2": 3,
	})
	resp := testTransact(t, req, testSchemaSimple, 1)
	validateSelectResult(t, resp, 2, 0, 1)
	row := (*resp.Result[0].Rows)[0]
	assert.Equal(t, "val1", row["key1"])
	assert.EqualValues(t, 3, row["key2"])

	validateEmptyResult(t, resp, 2, 1)
	testEtcdGetComment(t, comment)
}

func TestTransactComment(t *testing.T) {
	comment := "ovs-vsctl add-br br0"
	req := &libovsdb.Transact{
		DBName: "simple",
		Operations: []libovsdb.Operation{
			{
				Op:      libovsdb.OperationComment,
				Comment: &comment,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaSimple, -1)
	assert.Nil(t, resp.Error)
	testEtcdGetComment(t, comment)
}

func TestTransactAssert(t *testing.T) {
}

func TestEqualMap(t *testing.T) {
	expectedValue := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"max_tunid": "16711680", "northd_internal_version": "21.03.1-20.16.1-56.0",
		"svc_monitor_mac": "4e:15:a4:a3:c8:7c", "use_logical_dp_groups": "true", "mac_prefix": "26:20:60"}}

	actualValue := libovsdb.OvsMap{GoMap: map[interface{}]interface{}{"svc_monitor_mac": "4e:15:a4:a3:c8:7c",
		"use_logical_dp_groups": "true", "mac_prefix": "26:20:60", "max_tunid": "16711680", "northd_internal_version": "21.03.1-20.16.1-56.0"}}

	colSchema, err := testSchemaMap.LookupColumn("table1", "string")
	assert.Nil(t, err)
	retValue := isEqualColumn(colSchema, expectedValue, actualValue, klogr.New())
	assert.True(t, retValue)
}

func TestEqualSet(t *testing.T) {
	uuidStr := "a56634ba-2324-4000-9164-2a7f3bdba133"
	uuid := libovsdb.UUID{GoUUID: uuidStr}
	log := klogr.New()
	colSchema, err := testSchemaSet.LookupColumn("table1", "uuid")
	assert.Nil(t, err)

	var expectedValue interface{}
	var actualValue interface{}
	expectedValue = libovsdb.OvsSet{GoSet: []interface{}{uuid}}
	actualValue = libovsdb.OvsSet{GoSet: []interface{}{uuid}}

	assert.True(t, isEqualColumn(colSchema, expectedValue, actualValue, log), "2 sets")

	expectedValue = uuid
	assert.True(t, isEqualColumn(colSchema, expectedValue, actualValue, log), "expected object")

	actualValue = uuid
	assert.True(t, isEqualColumn(colSchema, expectedValue, actualValue, log), "2 objects")

	expectedValue = libovsdb.OvsSet{GoSet: []interface{}{uuid}}
	assert.True(t, isEqualColumn(colSchema, expectedValue, actualValue, log), "actual object")
}

func prepareValues4GC(t *testing.T, rootTable, table1, table2, dbName string) (uuidR, uuid1, uuid2 *libovsdb.UUID) {
	table1UUIDNamed := "named-uuid1"
	table2UUIDNamed := "named-uuid2"
	table1RowUUID := libovsdb.UUID{GoUUID: table1UUIDNamed}
	table2RowUUID := libovsdb.UUID{GoUUID: table2UUIDNamed}

	rootRow := map[string]interface{}{
		"name":   "root",
		"refSet": libovsdb.OvsSet{GoSet: []interface{}{table1RowUUID}},
	}
	table1Row := map[string]interface{}{
		"name":   "t1",
		"refSet": libovsdb.OvsSet{GoSet: []interface{}{table2RowUUID}},
	}

	table2Row := map[string]interface{}{
		"name":  "t2",
		"_uuid": table2RowUUID,
	}

	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationInsert,
				Table: &rootTable,
				Row:   &rootRow,
			},
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table1,
				Row:      &table1Row,
				UUIDName: &table1UUIDNamed,
			},
			{
				Op:       libovsdb.OperationInsert,
				Table:    &table2,
				Row:      &table2Row,
				UUIDName: &table2UUIDNamed,
			},
		},
	}
	testEtcdCleanup(t)
	resp := testTransact(t, req, testSchemaGC, 0)
	validateInsertResult(t, resp, 3, 0, "")
	uuidR = resp.Result[0].UUID
	validateInsertResult(t, resp, 3, 1, "")
	uuid1 = resp.Result[1].UUID
	validateInsertResult(t, resp, 3, 2, "")
	uuid1 = resp.Result[2].UUID
	return
}

func TestTransactGCUpdate(t *testing.T) {
	rootTable := "rootTable"
	table1 := "table1"
	table2 := "table2"
	dbName := "gc"
	uuidR, uuid1, _ := prepareValues4GC(t, rootTable, table1, table2, dbName)

	newRootRow := map[string]interface{}{"name": "root", "refSet": libovsdb.OvsSet{GoSet: []interface{}{libovsdb.UUID{GoUUID: uuid.NewString()}}}}
	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationUpdate,
				Table: &rootTable,
				Row:   &newRootRow,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuidR}},
			},
		},
	}
	resp := testTransact(t, req, testSchemaGC, 3)
	validateUpdateResult(t, resp, 1, 0, 1)
	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table1,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid1}},
			},
			{
				Op:    libovsdb.OperationSelect,
				Table: &table2,
			},
		},
	}
	resp = testTransact(t, req, testSchemaGC, 1)
	validateSelectResult(t, resp, 2, 0, 0)
	validateSelectResult(t, resp, 2, 1, 0)
}

func TestTransactGCDelete(t *testing.T) {
	rootTable := "rootTable"
	table1 := "table1"
	table2 := "table2"
	dbName := "gc"
	uuidR, uuid1, _ := prepareValues4GC(t, rootTable, table1, table2, dbName)

	req := &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationDelete,
				Table: &rootTable,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuidR}},
			},
		},
	}
	resp := testTransact(t, req, testSchemaGC, 3)
	validateUpdateResult(t, resp, 1, 0, 1)
	req = &libovsdb.Transact{
		DBName: dbName,
		Operations: []libovsdb.Operation{
			{
				Op:    libovsdb.OperationSelect,
				Table: &table1,
				Where: &[]interface{}{[]interface{}{libovsdb.ColUuid, FuncEQ, *uuid1}},
			},
			{
				Op:    libovsdb.OperationSelect,
				Table: &table2,
			},
		},
	}
	resp = testTransact(t, req, testSchemaGC, 0)
	validateSelectResult(t, resp, 2, 0, 0)
	validateSelectResult(t, resp, 2, 1, 0)
}
