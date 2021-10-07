package ovsdb

import (
	"context"
	"fmt"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"sync"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
)

type cachedRow struct {
	row     libovsdb.Row
	version int64
	key     string
	counter int
}

type cache map[string]*databaseCache

type databaseCache struct {
	mu      sync.RWMutex
	dbCache map[string]tableCache
	log     logr.Logger
}

func newDatabaseCache(dSchema *libovsdb.DatabaseSchema, log logr.Logger) databaseCache {
	dCache := databaseCache{dbCache: map[string]tableCache{}, log: log}
	for tName, tSchema := range dSchema.Tables {
		dCache.dbCache[tName] = newTableCache(tSchema)
	}
	return dCache
}

type tableCache struct {
	rows       map[string]cachedRow
	refColumns []string
	// TODO add secondary index
}

func newTableCache(tSchema libovsdb.TableSchema) tableCache {
	var rColumns []string
	for cn, colSchema := range tSchema.Columns {
		if colSchema.TypeObj != nil {
			if colSchema.TypeObj.Key != nil && colSchema.TypeObj.Key.RefTable != "" {
				rColumns = append(rColumns, cn)
			}
			if colSchema.TypeObj.Value != nil && colSchema.TypeObj.Value.RefTable != "" {
				rColumns = append(rColumns, cn)
			}
		}
	}
	return tableCache{rows: map[string]cachedRow{}, refColumns: rColumns}
}

func (tc *tableCache) newEmptyTableCache() *tableCache {
	return &tableCache{rows: map[string]cachedRow{}, refColumns: tc.refColumns}
}

func (tc *tableCache) size() int {
	return len(tc.rows)
}

func (c *cache) addDatabaseCache(dbSchema *libovsdb.DatabaseSchema, etcdClient *clientv3.Client, log logr.Logger) error {
	dbName := dbSchema.Name
	if _, ok := (*c)[dbName]; ok {
		return errors.New("Duplicate DatabaseCache: " + dbName)
	}
	dbCache := newDatabaseCache(dbSchema, log)
	(*c)[dbName] = &dbCache
	if dbName == INT_SERVER {
		return nil
	}
	ctxt := context.Background()
	key := common.NewDBPrefixKey(dbName)
	resp, err := etcdClient.Get(ctxt, key.String(), clientv3.WithPrefix())
	if err != nil {
		log.Error(err, "get KeyData")
		return err
	}
	err = dbCache.storeValues(resp.Kvs)
	wch := etcdClient.Watch(clientv3.WithRequireLeader(ctxt), key.String(),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithRev(resp.Header.Revision))

	if err != nil {
		log.Error(err, "storeValues")
		return err
	}

	go func() {
		// TODO propagate to monitors
		for wresp := range wch {
			if wresp.Canceled {
				log.V(1).Info("DB cache monitor was canceled", "dbName", dbName)
				// TODO: reconnect ?
				return
			}
			dbCache.updateCache(wresp.Events)
		}
	}()
	return nil
}

func newCachedRow(key string, value []byte, version int64) (*cachedRow, error) {
	row := libovsdb.Row{}
	err := row.UnmarshalJSON(value)
	if err != nil {
		return nil, errors.WithMessagef(err, "unmarshal to row, key %s", key)
	}
	return &cachedRow{row: row, key: key, version: version}, nil
}

func (dc *databaseCache) updateCache(events []*clientv3.Event) {
	// we want to minimize the lock time, so we first prepare ALL the rows, and only after that update the cache
	rows  := map[common.Key]*cachedRow{}
	for _, event := range events {
		strKey := string(event.Kv.Key)
		key, err := common.ParseKey(strKey)
		if err != nil {
			dc.log.Error(err, "got a wrong formatted key from etcd", "key", strKey)
			continue
		}
		if key.IsCommentKey() {
			continue
		}
		if event.Type == mvccpb.DELETE {
			rows[*key] = nil
		} else {
			cr, err := newCachedRow(strKey, event.Kv.Value, event.Kv.Version)
			if err != nil {
				dc.log.Error(err, "cannot update cache value")
				continue
			}
			rows[*key] = cr
		}
	}
	// now actually update the cache
	dc.updateRows(rows)
}

func (dc *databaseCache) storeValues(kvs []*mvccpb.KeyValue) error {
	rows  := map[common.Key]*cachedRow{}
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

		cr, err := newCachedRow(key.String(), kv.Value, kv.Version)
		if err != nil {
			dc.log.Error(err, "cannot store value in the cache")
			return err
		}
		// TODO update counters
		rows[*key] = cr
	}
	// now actually update the cache
	dc.updateRows(rows)
	return nil
}

func (dc *databaseCache) updateRows(newRows map[common.Key]*cachedRow) {
	// now update the cache
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for key, row := range newRows {
		tb := dc.getTable(key.TableName)
		if row == nil {
			// delete event
			// TODO update counters
			delete(tb.rows, key.UUID)
		} else {
			// TODO update counters
			tb.rows[key.UUID] = *row
		}
	}
}


// should be called under locked dc.mu
func (dc *databaseCache) getTable(tableName string) *tableCache {
	tb, ok := dc.dbCache[tableName]
	if !ok {
		panic(fmt.Sprintf("There is no table %s in the cache", tableName))
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
