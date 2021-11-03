package ovsdb

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/ibm/ovsdb-etcd/pkg/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type cacheListener interface {
	updateCache([]*clientv3.Event)
}

type dbWatcher struct {
	// etcdTrx watcher channel
	watchChannel clientv3.WatchChan
	// cancel function to close the etcd watcher
	cancel           context.CancelFunc
	cacheListener    cacheListener
	monitorListeners []*dbMonitor
	mu               sync.Mutex
	log     		logr.Logger
}

func CreateDBWatcher(dbName string, cli *clientv3.Client, revision int64, logger logr.Logger) *dbWatcher {
	// TODO propagate context and cancel function (?)
	ctxt, cancel := context.WithCancel(context.Background())
	key := common.NewDBPrefixKey(dbName)
	wch := cli.Watch(clientv3.WithRequireLeader(ctxt), key.String(),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(revision),
	)
	return &dbWatcher{watchChannel: wch, cancel: cancel, log: logger}
}

func (dbw *dbWatcher) addMonitor(dbm *dbMonitor) {
	dbw.mu.Lock()
	defer dbw.mu.Unlock()
	dbw.monitorListeners = append(dbw.monitorListeners, dbm)
}

func (dbw *dbWatcher) removeMonitor(dbm *dbMonitor) {
	dbw.mu.Lock()
	defer dbw.mu.Unlock()
	for i, listener := range dbw.monitorListeners {
		if listener == dbm {
			dbw.monitorListeners = append(dbw.monitorListeners[:i], dbw.monitorListeners[i+1:]...)
			return
		}
	}
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
	if dbw.cacheListener != nil {
		dbw.cacheListener.updateCache(events)
	}
	ovsdbEvents := make([]*ovsdbNotificationEvent, 0, len(events))
	for _, event := range events {
		ovsdbEvent, err := etcd2ovsdbEvent(event, dbw.log)
		if err != nil {
			mvccpbEvent := mvccpb.Event(*event)
			dbw.log.Error(err, "etcd2ovsdbEvent returned", "event", mvccpbEvent.String())
			continue
		}
		ovsdbEvents = append(ovsdbEvents, ovsdbEvent)
	}
	for _, l := range dbw.monitorListeners {
		go l.notify(ovsdbEvents, revision)
	}
}
