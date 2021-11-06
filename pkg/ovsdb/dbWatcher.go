package ovsdb

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"github.com/ibm/ovsdb-etcd/pkg/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type cacheListener interface {
	updateRows(events map[common.Key]*cachedRow)
}

type dbWatcher struct {
	// etcdTrx watcher channel
	watchChannel     clientv3.WatchChan
	cacheListener    cacheListener
	monitorListeners map[*dbMonitor]*dbMonitor
	mu               sync.Mutex
	log              logr.Logger
}

func CreateDBWatcher(dbName string, cli *clientv3.Client, revision int64, logger logr.Logger) *dbWatcher {
	// TODO propagate context and cancel function (?)
	key := common.NewDBPrefixKey(dbName)
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())
	opts = append(opts, clientv3.WithCreatedNotify())
	opts = append(opts, clientv3.WithPrevKV())
	if revision > -1 {
		opts = append(opts, clientv3.WithRev(revision))
	}
	wch := cli.Watch(clientv3.WithRequireLeader(context.Background()), key.String(), opts...)
	return &dbWatcher{watchChannel: wch, monitorListeners: make(map[*dbMonitor]*dbMonitor), log: logger}
}

func (dbw *dbWatcher) addMonitor(dbm *dbMonitor) {
	dbw.mu.Lock()
	defer dbw.mu.Unlock()
	dbw.monitorListeners[dbm] = dbm
}

func (dbw *dbWatcher) removeMonitor(dbm *dbMonitor) {
	dbw.mu.Lock()
	defer dbw.mu.Unlock()
	delete(dbw.monitorListeners, dbm)
}

func (dbw *dbWatcher) start() {
	go func() {
		// TODO propagate to monitors
		for wresp := range dbw.watchChannel {
			if wresp.Canceled {
				// TODO: reconnect ?
				return
			}
			dbw.notify(wresp.Events, wresp.Header.Revision)
		}
	}()
}

func (dbw *dbWatcher) notify(events []*clientv3.Event, revision int64) {
	// TODO combine ovsdbEvents and newCachedRows
	ovsdbEvents := make([]*ovsdbNotificationEvent, 0, len(events))
	newCachedRows := map[common.Key]*cachedRow{}
	for _, event := range events {
		ovsdbEvent, err := etcd2ovsdbEvent(event, dbw.log)
		if err != nil {
			mvccpbEvent := mvccpb.Event(*event)
			dbw.log.Error(err, "etcd2ovsdbEvent returned", "event", mvccpbEvent.String())
			continue
		}
		ovsdbEvents = append(ovsdbEvents, ovsdbEvent)
		if !ovsdbEvent.modifyEvent && !ovsdbEvent.createEvent {
			newCachedRows[ovsdbEvent.key] = nil
		} else {
			newCachedRows[ovsdbEvent.key] = &cachedRow{key: ovsdbEvent.key.String(), row: ovsdbEvent.row, version: event.Kv.Version}
		}
	}
	if dbw.cacheListener != nil && len(newCachedRows) > 0 {
		go dbw.cacheListener.updateRows(newCachedRows)
	}
	dbw.mu.Lock()
	defer dbw.mu.Unlock()
	for l := range dbw.monitorListeners {
		go l.notify(ovsdbEvents, revision)
	}
}
