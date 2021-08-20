package ovsdb

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
)

type cache map[string]*databaseCache

type databaseCache struct {
	mu      sync.Mutex
	dbCache map[string]tableCache
	// etcdTrx watcher channel
	watchChannel clientv3.WatchChan
	// cancel function to close the etcdTrx watcher
	cancel context.CancelFunc
	log    logr.Logger
}

type tableCache map[string]*mvccpb.KeyValue

func (tc *tableCache) size() int {
	return len(*tc)
}

func (c *cache) addDatabaseCache(dbName string, etcdClient *clientv3.Client, log logr.Logger) error {
	if _, ok := (*c)[dbName]; ok {
		return errors.New("Duplicate DatabaseCashe: " + dbName)
	}
	dbCache := databaseCache{dbCache: map[string]tableCache{}, log: log}
	ctxt, cancel := context.WithCancel(context.Background())
	dbCache.cancel = cancel
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
	dbCache.watchChannel = wch
	(*c)[dbName] = &dbCache
	dbCache.putEtcdKV(resp.Kvs)
	go func() {
		// TODO propagate to monitors
		for wresp := range dbCache.watchChannel {
			if wresp.Canceled {
				// TODO: reconnect ?
				return
			}
			dbCache.updateCache(wresp.Events)
		}
	}()
	return nil
}

func (dc *databaseCache) updateCache(events []*clientv3.Event) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for _, event := range events {
		key, err := common.ParseKey(string(event.Kv.Key))
		if err != nil {
			dc.log.Error(err, "got a wrong formatted key from etcd", "key", string(event.Kv.Key))
			continue
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

func (dc *databaseCache) putEtcdKV(kvs []*mvccpb.KeyValue) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		key, err := common.ParseKey(string(kv.Key))
		if err != nil {
			return err
		}
		if key.IsCommentKey() {
			continue
		}
		tb := dc.getTable(key.TableName)
		(*tb)[key.UUID] = kv
	}
	return nil
}

// should be called under locked dc.mu
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

func (c *cache) getDBCache(dbname string) *databaseCache {
	db, ok := (*c)[dbname]
	if !ok {
		db = &databaseCache{dbCache: map[string]tableCache{}}
		(*c)[dbname] = db
	}
	return db
}
