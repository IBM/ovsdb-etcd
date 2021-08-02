package ovsdb

import (
	"github.com/go-logr/logr"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

type cache map[string]databaseCache

type databaseCache map[string]tableCache

// todo add revision
type tableCache map[string]*map[string]interface{}

func (c *cache) getDatabase(dbname string) databaseCache {
	db, ok := (*c)[dbname]
	if !ok {
		db = databaseCache{}
		(*c)[dbname] = db
	}
	return db
}

func (c *cache) getTable(dbname, table string) tableCache {
	db := c.getDatabase(dbname)
	tb, ok := db[table]
	if !ok {
		tb = tableCache{}
		db[table] = tb
	}
	return tb
}

func (c *cache) Row(key common.Key) *map[string]interface{} {
	tb := c.getTable(key.DBName, key.TableName)
	_, ok := tb[key.UUID]
	if !ok {
		tb[key.UUID] = new(map[string]interface{})
	}
	return tb[key.UUID]
}

func (c *cache) GetFromEtcdKV(kvs []*mvccpb.KeyValue) error {
	for _, x := range kvs {
		kv, err := NewKeyValue(x)
		if err != nil {
			return err
		}
		row := c.Row(kv.Key)
		(*row) = kv.Value
	}
	return nil
}

func (cache *cache) GetFromEtcd(res *clientv3.TxnResponse) {
	for _, r := range res.Responses {
		switch v := r.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			cache.GetFromEtcdKV(v.ResponseRange.Kvs)
		}
	}
}

func (cache *cache) Unmarshal(log logr.Logger, schema *libovsdb.DatabaseSchema) error {
	// TODO remove database
	for _, databaseCache := range *cache {
		for table, tableCache := range databaseCache {
			for _, row := range tableCache {
				err := schema.Unmarshal(table, row)
				if err != nil {
					log.Error(err, "failed schema unmarshal")
					return err
				}
			}
		}
	}
	return nil
}

func (cache *cache) Validate(dataBase string, schema *libovsdb.DatabaseSchema, log logr.Logger) error {
	databaseCache := cache.getDatabase(dataBase)
	for table, tableCache := range databaseCache {
		for _, row := range tableCache {
			err := schema.Validate(table, row)
			if err != nil {
				log.Error(err, "failed schema validate")
				return err
			}
		}
	}
	return nil
}
