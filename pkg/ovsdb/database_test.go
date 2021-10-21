package ovsdb

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	etcdClient "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

func TestMockAddSchema(t *testing.T) {
	var expectedError error
	mock := DatabaseMock{
		Error: expectedError,
	}
	actualError := mock.AddSchema("")
	assert.Equal(t, expectedError, actualError)
}

func TestMockGetData(t *testing.T) {
	var expectedResponse *etcdClient.GetResponse
	var expectedError error
	mock := DatabaseMock{
		Error:    expectedError,
		Response: expectedResponse,
	}
	actualResponse, actualError := mock.GetKeyData(common.GenerateDataKey("dbName", "tableName"), true)
	assert.Equal(t, expectedError, actualError)
	assert.Equal(t, expectedResponse, actualResponse)
}

func TestDatabaseSetDatabase(t *testing.T) {
	checkEmptySet := func(t *testing.T, v interface{}) {
		set, ok := v.(libovsdb.OvsSet)
		assert.True(t, ok)
		assert.True(t, len(set.GoSet) == 0)
	}
	checkUUID := func(t *testing.T, v interface{}) {
		uuid, ok := v.(libovsdb.UUID)
		assert.True(t, ok)
		assert.True(t, len(uuid.GoUUID) > 0)
	}
	checkDatabaseRow := func(t *testing.T, row map[string]interface{}, dbName string, expectedModel string) {
		v, ok := row[libovsdb.ColUuid]
		assert.True(t, ok)
		checkUUID(t, v)

		v, ok = row[libovsdb.ColVersion]
		assert.True(t, ok)
		checkUUID(t, v)

		v, ok = row[DBColName]
		assert.True(t, ok)
		assert.Equal(t, dbName, v)

		v, ok = row[DBColLeader]
		assert.True(t, ok)
		if expectedModel == ModStandalone {
			assert.True(t, v.(bool))
		} else {
			assert.False(t, v.(bool))
		}

		v, ok = row[DBColModel]
		assert.True(t, ok)
		assert.Equal(t, expectedModel, v)

		v, ok = row[DBColCID]
		assert.True(t, ok)
		if expectedModel == ModStandalone {
			checkEmptySet(t, v)
		} else {
			// TODO
		}

		v, ok = row[DBColSID]
		assert.True(t, ok)
		if expectedModel == ModStandalone {
			checkEmptySet(t, v)
		} else {
			// TODO
		}

		v, ok = row[DBColIndex]
		assert.True(t, ok)
		if expectedModel == ModStandalone {
			checkEmptySet(t, v)
		} else {
			// TODO
		}
	}
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer cli.Close()

	checkDBModels := func(t *testing.T, model string) {
		dbs, err := NewDatabaseEtcd(cli, model, log)
		assert.Nil(t, err)
		db := dbs.(*DatabaseEtcd)
		err = db.AddSchema(path.Join("../../schemas", "_server.ovsschema"))
		assert.Nil(t, err)
		err = db.AddSchema(path.Join("../../schemas", "ovn-nb.ovsschema"))
		assert.Nil(t, err)
		dbCache, err := db.GetDBCache(IntServer)
		assert.Nil(t, err)

		tCache := dbCache.getTable(IntDatabase)
		cRow, ok := tCache.rows[IntServer]
		assert.True(t, ok)
		checkDatabaseRow(t, cRow.row.Fields, IntServer, ModStandalone)
		cRow, ok = tCache.rows[db.dbName]
		assert.True(t, ok)
		checkDatabaseRow(t, cRow.row.Fields, db.dbName, db.model)
	}
	checkDBModels(t, ModStandalone)
	checkDBModels(t, ModClustered)
}

func TestDatabaseEtcdLeaderElection(t *testing.T) {
	size := 3
	dbs := make([]*DatabaseEtcd, size, size)
	cli := make([]*etcdClient.Client, size, size)
	var err error
	for i := 0; i < size; i++ {
		cli[i], err = testEtcdNewCli()
		assert.Nil(t, err)
	}
	defer func() {
		for i := 0; i < size; i++ {
			if cli[i] != nil {
				err = cli[i].Close()
				assert.Nil(t, err)
			}
		}
	}()
	for i := 0; i < size; i++ {
		db, err := NewDatabaseEtcd(cli[i], ModClustered, log)
		assert.Nil(t, err)
		dbs[i] = db.(*DatabaseEtcd)
		err = db.AddSchema(path.Join("../../schemas", "_server.ovsschema"))
		assert.Nil(t, err)
		err = db.AddSchema(path.Join("../../schemas", "ovn-nb.ovsschema"))
		assert.Nil(t, err)
	}
	for i := 0; i < size; i++ {
		assert.False(t, isLeader(t, dbs[i]))
	}
	start := time.Now()
	for i := 0; i < size; i++ {
		dbs[i].StartLeaderElection()
	}
	log.Info("Leader election is done")
	leader := -1
	for j := 0; j < 10; j++ {
		for i := 0; i < size; i++ {
			l := isLeader(t, dbs[i])
			if l {
				assert.Equal(t, leader, -1)
				leader = i
			}
		}
		if leader != -1 {
			log.Info(fmt.Sprintf("leader is %d, time %v", leader, time.Now().Sub(start)))
			break
		} else {
			time.Sleep(time.Duration(10*j) * time.Millisecond)
		}
	}
	assert.NotEqual(t, leader, -1)
	/*  uncomment to validate leader election over a server failure.
	start = time.Now()
	cli[leader].Close()
	cli[leader] = nil
	oldLeader := leader
	leader = -1
	log.Info("start reelection loop")
	for j := 0; j < 100; j++ {
		for i := 0; i < size; i++ {
			if i == oldLeader {
				continue
			}
			l := isLeader(t, dbs[i])
			if l {
				assert.NotEqual(t, oldLeader, i)
				assert.NotEqual(t, -1, i)
				leader = i
			}
		}
		if leader != -1 {
			log.Info(fmt.Sprintf("New leader %d %v\n", leader, time.Now().Sub(start)))
			break
		} else {
			time.Sleep(time.Duration(1*j) * time.Second)
		}
	}
	*/
}

func isLeader(t *testing.T, db *DatabaseEtcd) bool {
	dbCache, err := db.GetDBCache(IntServer)
	assert.Nil(t, err)
	tCache := dbCache.getTable(IntDatabase)
	cRow, ok := tCache.rows[db.dbName]
	assert.True(t, ok)
	fmt.Printf("row %v\n", cRow.row.Fields)
	leader, ok := cRow.row.Fields[DBColLeader]
	if !ok {
		return false
	}
	return leader.(bool)
}
