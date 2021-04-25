package ovsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"
)

type Databaser interface {
	GetLock(ctx context.Context, id string) (Locker, error)
	AddMonitors(dbName string, updaters map[string][]*updater, handler handlerKey)
	RemoveMonitors(dbName string, updaters map[string][]string, handler handlerKey)
	RemoveMonitor(dbName string)
	AddSchema(schemaName, schemaFile string) error
	GetData(prefix string, keysOnly bool) (*clientv3.GetResponse, error)
	PutData(ctx context.Context, key string, obj interface{}) error
	GetSchema(name string) (string, bool)
}

type DatabaseEtcd struct {
	cli *clientv3.Client
	// dataBaseName -> schema
	Schemas map[string]string
	mu      sync.Mutex
	prefix  string
	// databaseName -> monitor
	// We have a single monitor (etcd watcher) per database
	monitors map[string]*monitor
}

type Locker interface {
	tryLock() error
	lock() error
	unlock() error
	cancel()
}

type lock struct {
	mutex    *concurrency.Mutex
	myCancel context.CancelFunc
	cntx     context.Context
}

func (l *lock) tryLock() error {
	return l.mutex.TryLock(l.cntx)
}

func (l *lock) lock() error {
	return l.mutex.Lock(l.cntx)
}

func (l *lock) unlock() error {
	return l.mutex.Unlock(l.cntx)
}

func (l *lock) cancel() {
	l.myCancel()
}

var EtcdClientTimeout = 100 * time.Millisecond

func NewDatabaseEtcd(cli *clientv3.Client, prefix string) (Databaser, error) {
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	return &DatabaseEtcd{cli: cli,
		Schemas: map[string]string{}, prefix: prefix, monitors: map[string]*monitor{}}, nil
}

func (con *DatabaseEtcd) GetLock(ctx context.Context, id string) (Locker, error) {
	ctctx, cancel := context.WithCancel(ctx)
	session, err := concurrency.NewSession(con.cli, concurrency.WithContext(ctctx))
	if err != nil {
		cancel()
		return nil, err
	}
	mutex := concurrency.NewMutex(session, con.prefix+"locks/"+id)
	return &lock{mutex: mutex, myCancel: cancel, cntx: ctctx}, nil
}

func (con *DatabaseEtcd) AddSchema(schemaName, schemaFile string) error {
	data, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	json.Compact(buffer, data)
	con.Schemas[schemaName] = buffer.String()
	return nil
}

func (con *DatabaseEtcd) GetData(keysPrefix string, keysOnly bool) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdClientTimeout)
	var resp *clientv3.GetResponse
	var err error
	if keysOnly {
		resp, err = con.cli.Get(ctx, con.prefix+keysPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	} else {
		resp, err = con.cli.Get(ctx, con.prefix+keysPrefix, clientv3.WithPrefix())
	}
	cancel()
	if err != nil {
		return nil, err
	}
	if klog.V(6).Enabled() {
		klog.Infof(" GetDatatype %T \n", resp.Kvs)
	}
	if klog.V(7).Enabled() {
		for k, v := range resp.Kvs {
			klog.V(7).Infof("GetData k %v, v %v\n", k, v)
		}
	}
	return resp, err
}

func (con *DatabaseEtcd) GetSchema(name string) (string, bool) {
	return con.Schemas[name], true
}

func (con *DatabaseEtcd) PutData(ctx context.Context, key string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = con.cli.Put(ctx, con.prefix+key, string(data))
	if err != nil {
		return err
	}
	return nil
}

func (con *DatabaseEtcd) AddMonitors(dbName string, updaters map[string][]*updater, handler handlerKey) {
	con.mu.Lock()
	defer con.mu.Unlock()
	m, ok := con.monitors[dbName]
	if !ok {
		// we don't have monitors for this database yet, create a new one.
		m = newMonitor(dbName, con)
		ctxt, cancel := context.WithCancel(context.Background())
		m.cancel = cancel
		wch := con.cli.Watch(clientv3.WithRequireLeader(ctxt), con.prefix+dbName,
			clientv3.WithPrefix(),
			clientv3.WithCreatedNotify(),
			clientv3.WithPrevKV())
		m.watchChannel = wch
		con.monitors[dbName] = m
	}
	m.addUpdaters(con.prefix, updaters, handler)
	if !ok {
		m.start()
	}
}

func (con *DatabaseEtcd) RemoveMonitors(dbName string, updaters map[string][]string, handler handlerKey) {
	con.mu.Lock()
	defer con.mu.Unlock()
	m, ok := con.monitors[dbName]
	if !ok {
		klog.Warningf("Remove nonexistent db monitor %s", dbName)
		return
	}
	m.removeUpdaters(con.prefix, updaters, handler)
	if !m.hasHandlers() {
		m.cancel()
		delete(con.monitors, dbName)
	}
}

func (con *DatabaseEtcd) RemoveMonitor(dbName string) {
	con.mu.Lock()
	defer con.mu.Unlock()
	delete(con.monitors, dbName)
}

type DatabaseMock struct {
	Response interface{}
	Error    error
	Ok       bool
	mu       sync.Mutex
}

type LockerMock struct {
	Mu    sync.Mutex
	Error error
}

func (l *LockerMock) tryLock() error {
	return l.Error
}

func (l *LockerMock) lock() error {
	l.Mu.Lock()
	return nil
}

func (l *LockerMock) unlock() error {
	l.Mu.Unlock()
	return nil
}

func (l *LockerMock) cancel() {
	l.Mu.Unlock()
}

func NewDatabaseMock() (Databaser, error) {
	return &DatabaseMock{}, nil
}

func (con *DatabaseMock) GetLock(ctx context.Context, id string) (Locker, error) {

	return &LockerMock{}, nil
}

func (con *DatabaseMock) AddSchema(schemaName, schemaFile string) error {
	return con.Error
}

func (con *DatabaseMock) GetData(keysPrefix string, keysOnly bool) (*clientv3.GetResponse, error) {
	return con.Response.(*clientv3.GetResponse), con.Error
}

func (con *DatabaseMock) PutData(ctx context.Context, key string, obj interface{}) error {
	return con.Error
}

func (con *DatabaseMock) GetSchema(name string) (string, bool) {
	return con.Response.(string), con.Ok
}

func (con *DatabaseMock) GetUUID() string {
	return con.Response.(string)
}

func (con *DatabaseMock) AddMonitors(dbName string, updaters map[string][]*updater, handler handlerKey) {
}

func (con *DatabaseMock) RemoveMonitors(dbName string, updaters map[string][]string, handler handlerKey) {
}

func (con *DatabaseMock) RemoveMonitor(dbName string) {
}
