package ovsdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type JrpcServerMock struct {
	notifyFunc func(ctx context.Context, method string, params interface{}) error
	t          *testing.T
}

func (jsm JrpcServerMock) Wait() error {
	return nil
}

func (jsm JrpcServerMock) Stop() {
}

func (jsm JrpcServerMock) Notify(ctx context.Context, method string, params interface{}) error {
	return jsm.notifyFunc(ctx, method, params)
}

func TestHandlerLock2True(t *testing.T) {
	ctx := context.Background()
	ctxc, cancel := context.WithCancel(ctx)
	cli, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer func() {
		cancel()
		err := cli.Close()
		assert.Nil(t, err)
	}()
	handler := NewHandler(ctxc, nil, cli, log)
	defer handler.Cleanup()

	resp, err := handler.Lock(ctxc, "myLock")
	assert.Nil(t, err)
	validateLockResponse(t, resp, true)

}

func TestHandlerLockFalse(t *testing.T) {
	lockID := "myLock"

	ctx := context.Background()
	ctxc, cancel := context.WithCancel(ctx)
	cli1, err := testEtcdNewCli()
	assert.Nil(t, err)
	cli2, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer func() {
		cancel()
		err = cli1.Close()
		assert.Nil(t, err)
		err = cli2.Close()
		assert.Nil(t, err)
	}()
	log1 := log.WithValues("handlerID", 1)
	handler1 := NewHandler(ctxc, nil, cli1, log1)
	defer handler1.Cleanup()
	log2 := log.WithValues("handlerID", 2)
	handler2 := NewHandler(ctxc, nil, cli2, log2)
	defer handler2.Cleanup()

	flag := false
	// add notification checkers
	handler2.jrpcServer = JrpcServerMock{t: t,
		notifyFunc: func(ctx context.Context, method string, params interface{}) error {
			assert.Equal(t, locked, method)
			str, ok := params.([]string)
			assert.True(t, ok)
			assert.Equal(t, lockID, str[0])
			flag = true
			return nil
		}}
	resp, err := handler1.Lock(nil, lockID)
	assert.Nil(t, err)
	validateLockResponse(t, resp, true)
	resp, err = handler2.Lock(nil, lockID)
	assert.Nil(t, err)
	validateLockResponse(t, resp, false)
	handler1.Unlock(nil, lockID)
	assert.Eventually(t, func() bool { return flag }, time.Second, 10*time.Millisecond)
}

func TestHandlerLockTrue(t *testing.T) {
	lockID := "myLock"

	ctx := context.Background()
	ctxc, cancel := context.WithCancel(ctx)
	cli1, err := testEtcdNewCli()
	assert.Nil(t, err)
	cli2, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer func() {
		cancel()
		err = cli1.Close()
		assert.Nil(t, err)
		err = cli2.Close()
		assert.Nil(t, err)
	}()
	log1 := log.WithValues("handlerID", 1)
	handler1 := NewHandler(ctxc, nil, cli1, log1)
	defer handler1.Cleanup()
	log2 := log.WithValues("handlerID", 2)
	handler2 := NewHandler(ctxc, nil, cli2, log2)
	defer handler2.Cleanup()

	resp, err := handler1.Lock(nil, lockID)
	assert.Nil(t, err)
	validateLockResponse(t, resp, true)
	_, err = handler1.Unlock(nil, lockID)
	assert.Nil(t, err)
	resp, err = handler2.Lock(nil, lockID)
	assert.Nil(t, err)
	validateLockResponse(t, resp, true)
}

func TestHandlerLockNotification(t *testing.T) {
	lockID := "myLock"

	ctx := context.Background()
	ctxc, cancel := context.WithCancel(ctx)
	cli1, err := testEtcdNewCli()
	assert.Nil(t, err)
	cli2, err := testEtcdNewCli()
	assert.Nil(t, err)
	defer func() {
		cancel()
		err = cli1.Close()
		assert.Nil(t, err)
		err = cli2.Close()
		assert.Nil(t, err)
	}()
	log1 := log.WithValues("handlerID", 1)
	handler1 := NewHandler(ctxc, nil, cli1, log1)
	defer handler1.Cleanup()
	log2 := log.WithValues("handlerID", 2)
	handler2 := NewHandler(ctxc, nil, cli2, log2)
	defer handler2.Cleanup()

	resp, err := handler1.Lock(nil, lockID)
	assert.Nil(t, err)
	validateLockResponse(t, resp, true)
	resp, err = handler2.Lock(nil, lockID)
	assert.Nil(t, err)
	validateLockResponse(t, resp, false)
}

func validateLockResponse(t *testing.T, resp interface{}, expectValue bool) {
	respMap, ok := resp.(map[string]bool)
	assert.True(t, ok)
	assert.Equal(t, expectValue, respMap[locked])
}
