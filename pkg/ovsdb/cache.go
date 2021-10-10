package ovsdb

import (
	"context"
	"fmt"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"reflect"
	"sync"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
)

type cachedRow struct {
	row     libovsdb.Row
	// etcd version of the row data
	version int64
	key     string
	// number of references to the row, relevant to rows in none root tables.
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
	for tName := range dSchema.Tables {
		dCache.dbCache[tName] = newTableCache(tName, dSchema)
	}
	return dCache
}

// stores row indexed by uuid, except _Server.Database entries, which are indexed by database name.
type tableCache struct {
	rows       map[string]cachedRow
	tSchema    *libovsdb.TableSchema
    // ref from this table columns to none root tables
	refColumns map[string]string
	// TODO add secondary index
}

func newTableCache(tableName string, dSchema *libovsdb.DatabaseSchema) tableCache {
	rColumns := map[string]string{}
	tSchema, ok := dSchema.Tables[tableName]
	if !ok {
		panic("cannot find table schema for " + tableName)
	}

	for cn, colSchema := range tSchema.Columns {
		if colSchema.TypeObj != nil {
			// the same column cannot contain references as key and as value
			if colSchema.TypeObj.Key != nil && colSchema.TypeObj.Key.RefTable != "" {
				refTable := colSchema.TypeObj.Key.RefTable
				if !dSchema.Tables[refTable].IsRoot {
					// we are aware about references to none root tables only
					rColumns[cn] = refTable
				}
			}
			if colSchema.TypeObj.Value != nil && colSchema.TypeObj.Value.RefTable != "" {
				refTable := colSchema.TypeObj.Value.RefTable
				if !dSchema.Tables[refTable].IsRoot {
					// we are aware about references to none root tables only
					rColumns[cn] = refTable
				}
			}
		}
	}
	return tableCache{rows: map[string]cachedRow{}, refColumns: rColumns, tSchema: &tSchema}
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
	// we don't need etcd watcher
	if etcdClient == nil {
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
		rows[*key] = cr
	}
	// now actually update the cache
	dc.updateRows(rows)
	return nil
}
/*
func (dc *databaseCache) deleteCounters(newVal interface{}, oldVal interface{}, columnType string, refTable *tableCache ) {
	if columnType == libovsdb.TypeSet {
		valSet, ok := newVal.(libovsdb.OvsSet)
		if !ok {
			// TODO
		}
		for _, uuid := range valSet.GoSet {
			ovsUUID := uuid.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter--
			}
		}
	} else if columnType == libovsdb.TypeMap {
		valMap, ok := newVal.(libovsdb.OvsMap)
		if !ok {
			// TODO
		}
		for _, v := range valMap.GoMap {
			ovsUUID, _ := v.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter--
			}
		}
	}
}
*/


func (dc *databaseCache) updateCountersSet(newVal interface{}, oldVal interface{}, refTable *tableCache ) {
	var newValSet libovsdb.OvsSet
	var oldValSet libovsdb.OvsSet
	var ok bool
	if newVal == nil {
		newValSet = libovsdb.OvsSet{}
	} else {
		newValSet, ok = newVal.(libovsdb.OvsSet)
		if !ok {
			// TODO
		}
	}
	if oldVal == nil {
		oldValSet = libovsdb.OvsSet{}
	} else {
		oldValSet, ok = newVal.(libovsdb.OvsSet)
		if !ok {
			// TODO
		}
	}
	for _, uuid := range newValSet.GoSet {
		if !oldValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter++
			}
		}
	}
	for _, uuid := range oldValSet.GoSet {
		if !newValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter--
			}
		}
	}
}

func (tc *tableCache) checkCountersSet(newVal interface{}, oldVal interface{} ) map[string]int {
	var newValSet libovsdb.OvsSet
	var oldValSet libovsdb.OvsSet
	var ok bool
	if newVal == nil {
		newValSet = libovsdb.OvsSet{}
	} else {
		newValSet, ok = newVal.(libovsdb.OvsSet)
		if !ok {
			// TODO
		}
	}
	if oldVal == nil {
		oldValSet = libovsdb.OvsSet{}
	} else {
		oldValSet, ok = newVal.(libovsdb.OvsSet)
		if !ok {
			// TODO
		}
	}
	counters := map[string]int{}
	for _, uuid := range newValSet.GoSet {
		if !oldValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]++
		}
	}
	for _, uuid := range oldValSet.GoSet {
		if !newValSet.ContainElement(uuid) {
			ovsUUID := uuid.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]--
		}
	}
	return counters
}

func (dc *databaseCache) updateCountersMap(newVal interface{}, oldVal interface{}, refTable *tableCache ) {
	var newValMap libovsdb.OvsMap
	var oldValMap libovsdb.OvsMap
	var ok bool
	if newVal == nil {
		newValMap = libovsdb.OvsMap{}
	} else {
		newValMap, ok = newVal.(libovsdb.OvsMap)
		if !ok {
			// TODO
		}
	}
	if oldVal == nil {
		oldValMap = libovsdb.OvsMap{}
	} else {
		oldValMap, ok = newVal.(libovsdb.OvsMap)
		if !ok {
			// TODO
		}
	}
	for k, newV := range newValMap.GoMap {
		oldV, ok := oldValMap.GoMap[k]
		if !ok {
			ovsUUID := newV.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter++
			}
		} else {
			if !reflect.DeepEqual(oldV, newV) {
				ovsUUID := newV.(libovsdb.UUID)
				cRow, ok := refTable.rows[ovsUUID.GoUUID]
				if ok {
					cRow.counter++
				}
				ovsUUID = oldV.(libovsdb.UUID)
				cRow, ok = refTable.rows[ovsUUID.GoUUID]
				if ok {
					cRow.counter--
				}
			}
		}
	}
	for k, oldV := range oldValMap.GoMap {
		_, ok := newValMap.GoMap[k]
		if !ok {
			ovsUUID := oldV.(libovsdb.UUID)
			cRow, ok := refTable.rows[ovsUUID.GoUUID]
			if ok {
				cRow.counter--
			}
		}
	}
}

func (tc *tableCache) checkCountersMap(newVal interface{}, oldVal interface{}) map[string]int {
	var newValMap libovsdb.OvsMap
	var oldValMap libovsdb.OvsMap
	var ok bool
	if newVal == nil {
		newValMap = libovsdb.OvsMap{}
	} else {
		newValMap, ok = newVal.(libovsdb.OvsMap)
		if !ok {
			// TODO
		}
	}
	if oldVal == nil {
		oldValMap = libovsdb.OvsMap{}
	} else {
		oldValMap, ok = newVal.(libovsdb.OvsMap)
		if !ok {
			// TODO
		}
	}
	counters := map[string]int{}
	for k, newV := range newValMap.GoMap {
		oldV, ok := oldValMap.GoMap[k]
		if !ok {
			ovsUUID := newV.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]++
		} else {
			if !reflect.DeepEqual(oldV, newV) {
				ovsUUID := newV.(libovsdb.UUID)
				counters[ovsUUID.GoUUID]++
				ovsUUID = oldV.(libovsdb.UUID)
				counters[ovsUUID.GoUUID]--
			}
		}
	}
	for k, oldV := range oldValMap.GoMap {
		_, ok := newValMap.GoMap[k]
		if !ok {
			ovsUUID := oldV.(libovsdb.UUID)
			ovsUUID = oldV.(libovsdb.UUID)
			counters[ovsUUID.GoUUID]--
		}
	}
	return counters
}

func (dc *databaseCache) updateCounters(newVal interface{}, oldVal interface{}, refTable *tableCache, columnType string) {
	switch columnType {
	case libovsdb.TypeSet:
		dc.updateCountersSet(newVal, oldVal, refTable)
	case libovsdb.TypeMap:
		dc.updateCountersMap(newVal, oldVal, refTable)
	case libovsdb.TypeUUID:
		// TODO
		default:
			//TODO
	}
}

func (tc *tableCache) checkCounters(newVal interface{}, oldVal interface{}, columnType string) map[string]int {
	switch columnType {
	case libovsdb.TypeSet:
		return tc.checkCountersSet(newVal, oldVal)
	case libovsdb.TypeMap:
		return tc.checkCountersMap(newVal, oldVal)
	case libovsdb.TypeUUID:
	// TODO
	default:
		//TODO
	}
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
			oldRow, ok := tb.rows[key.UUID]
			if !ok {
				continue
			}
			for cName, destTable := range tb.refColumns {
				val := oldRow.row.Fields[cName]
				if val == nil {
					continue
				}
				dTable := dc.getTable(destTable)
				cSchema, _ := tb.tSchema.LookupColumn(cName)
				dc.updateCounters(nil, val, dTable, cSchema.Type)
			}
			delete(tb.rows, key.UUID)
		} else {
			oldRow, ok := tb.rows[key.UUID]
			for cName, destTable := range tb.refColumns {
				newVal := row.row.Fields[cName]
				dTable := dc.getTable(destTable)
				cSchema, _ := tb.tSchema.LookupColumn(cName)
				if ok {
					dc.updateCounters(newVal, oldRow.row.Fields[cName], dTable, cSchema.Type)
				} else {
					dc.updateCounters(newVal, nil, dTable, cSchema.Type)
				}
			}
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
