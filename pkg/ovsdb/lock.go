package ovsdb

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type databaseLocks struct {
	sync.Map
}

func (dbLocks *databaseLocks) getLocker(id string) (*locker, error) {
	locI, ok := dbLocks.Load(id)
	if ok {
		myLock, ok := locI.(*locker)
		if !ok {
			return nil, fmt.Errorf("cannot transform to Locker, %T", locI)
		} else {
			return myLock, nil
		}
	}
	return nil, nil
}

func (dbLocks *databaseLocks) unlock(id string) error {
	locI, ok := dbLocks.LoadAndDelete(id)
	if !ok || locI == nil {
		return nil
	}
	myLock, ok := locI.(*locker)
	if !ok {
		return fmt.Errorf("cannot transform to Locker, %T", locI)
	}
	return myLock.unlock()
}

func (dbLocks *databaseLocks) cleanup(log logr.Logger) {
	dbLocks.Range(func(key, value interface{}) bool {
		mLock, ok := value.(*locker)
		if !ok {
			log.V(1).Info("cleanup, cannot transform to Locker", "type", fmt.Sprintf("%T", value), "lockID", key)
			return true
		}
		if err := mLock.unlock(); err != nil {
			log.Error(err, "cannot unlock", "lockID", key)
			return true
		}
		log.V(6).Info("Unlock", "lockID", key)
		dbLocks.Delete(key)
		return true
	})
}

type locker struct {
	locked   bool
	mutex    *concurrency.Mutex
	myCancel context.CancelFunc
	ctx      context.Context
}

func (l *locker) tryLock() error {
	err := l.mutex.TryLock(l.ctx)
	if err == nil {
		l.locked = true
	}
	return err
}

func (l *locker) lock() error {
	err := l.mutex.Lock(l.ctx)
	if err == nil {
		l.locked = true
	}
	return err
}

func (l *locker) unlock() error {
	l.locked = false
	return l.mutex.Unlock(l.ctx)
}

func (l *locker) cancel() {
	l.locked = false
	l.myCancel()
}

func (l *locker) isLocalLocked() bool {
	return l.locked
}

func (l *locker) isGlobalLocked() clientv3.Cmp {
	return l.mutex.IsOwner()
}
