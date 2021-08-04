package ovsdb

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

type cache map[string]databaseCache

type databaseCache struct {
	dbCache map[string]tableCache
	// etcdTrx watcher channel
	watchChannel clientv3.WatchChan
	// cancel function to close the etcdTrx watcher
	cancel context.CancelFunc
}

type tableCache map[string]*mvccpb.KeyValue

func (tc *tableCache) size() int {
	return len(*tc)
}

func (c *cache) addDatabaseCache(dbName string, etcdClient *clientv3.Client) error {
	if _, ok := (*c)[dbName]; ok {
		return errors.New("Duplicate DatabaseCashe: " + dbName)
	}
	dbCach := databaseCache{dbCache: map[string]tableCache{}}
	ctxt, cancel := context.WithCancel(context.Background())
	dbCach.cancel = cancel
	key := common.NewDBPrefixKey(dbName)
	resp, err := etcdClient.Get(ctxt, key.String(), clientv3.WithPrefix())
	if err != nil {
		klog.Errorf("GetKeyData: %s", err)
		return err
	}
	wch := etcdClient.Watch(clientv3.WithRequireLeader(ctxt), key.String(),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV())
	dbCach.watchChannel = wch
	(*c)[dbName] = dbCach
	(*c).PutEtcdKV(resp.Kvs)
	go func() {
		// TODO propagate to monitors
		for wresp := range dbCach.watchChannel {
			if wresp.Canceled {
				// TODO: reconnect ?
				return
			}
			dbCach.updateCache(wresp.Events)
		}
	}()
	return nil
}

func (dc *databaseCache) updateCache(events []*clientv3.Event) {
	for _, event := range events {
		key, err := common.ParseKey(string(event.Kv.Key))
		if err != nil {
			// TODO log
		}
		if key.IsCommentKey() {
			continue
		}
		tb := dc.getTable(key.TableName)
		if event.Type == mvccpb.DELETE {
			delete(*tb, key.UUID)
		} else {
			(*tb)[key.UUID] = event.Kv
		}
	}
}

func (dc *databaseCache) getTable(tableName string) *tableCache {
	tb, ok := dc.dbCache[tableName]
	if !ok {
		tb = tableCache{}
		dc.dbCache[tableName] = tb
	}
	return &tb
}

func (dc *databaseCache) size() int {
	var ret int
	for _, tbCache := range dc.dbCache {
		ret += tbCache.size()
	}
	return ret
}

func (c *cache) size() int {
	var ret int
	for _, dbCache := range *c {
		ret += dbCache.size()
	}
	return ret
}

func (c *cache) getDatabase(dbname string) *databaseCache {
	db, ok := (*c)[dbname]
	if !ok {
		db = databaseCache{dbCache: map[string]tableCache{}}
		(*c)[dbname] = db
	}
	return &db
}

func (c *cache) getTable(dbname, table string) *tableCache {
	db := c.getDatabase(dbname)
	tb, ok := db.dbCache[table]
	if !ok {
		tb = tableCache{}
		db.dbCache[table] = tb
	}
	return &tb
}

func (c *cache) Row(key common.Key) *mvccpb.KeyValue {
	tb := c.getTable(key.DBName, key.TableName)
	return (*tb)[key.UUID]
}

func (c *cache) PutEtcdKV(kvs []*mvccpb.KeyValue) error {
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		key, err := common.ParseKey(string(kv.Key))
		if err != nil {
			return err
		}
		tb := c.getTable(key.DBName, key.TableName)
		(*tb)[key.UUID] = kv
	}
	return nil
}

func (cache *cache) GetFromEtcd(res *clientv3.TxnResponse) {
	for _, r := range res.Responses {
		switch v := r.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			cache.PutEtcdKV(v.ResponseRange.Kvs)
		}
	}
}

func (cache *cache) Unmarshal(log logr.Logger, schema *libovsdb.DatabaseSchema) error {
	/*// TODO remove database
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
	}*/
	return nil
}

func (cache *cache) Validate(dataBase string, schema *libovsdb.DatabaseSchema, log logr.Logger) error {
	/*databaseCache := cache.getDatabase(dataBase)
	for table, tableCache := range databaseCache {
		for _, row := range tableCache {
			err := schema.Validate(table, row)
			if err != nil {
				log.Error(err, "failed schema validate")
				return err
			}
		}
	}*/
	return nil
}

/*
type KeyValue struct {
	Key   common.Key
	Value map[string]interface{}
}

func NewKeyValue(etcdKV *mvccpb.KeyValue) (*KeyValue, error) {
	if etcdKV == nil {
		return nil, fmt.Errorf("nil etcdKV")
	}
	kv := new(KeyValue)

	/ * key * /
	if etcdKV.Key == nil {
		return nil, fmt.Errorf("nil key")
	}
	key, err := common.ParseKey(string(etcdKV.Key))
	if err != nil {
		return nil, err
	}
	kv.Key = *key
	/ * value * /
	err = json.Unmarshal(etcdKV.Value, &kv.Value)
	if err != nil {
		return nil, err
	}

	return kv, nil
}
func ParseKey(etcdKV *mvccpb.KeyValue) ( *common.Key, error) {
	if etcdKV == nil {
		return nil, fmt.Errorf("nil etcdKV")
	}
	return common.ParseKey(string(etcdKV.Key))
}
*/
