package ovsdb

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type etcdConenctor struct {
	cli *clientv3.Client
}

func NewETCDConenctor( endpoints []string) (*etcdConenctor, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:  endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("NewETCDConenctor , error: ", err)
		return nil, err
	}
	// TODO
	//defer cli.Close()
	fmt.Printf("etcd client is connected")
	return &etcdConenctor{cli: cli}, nil
}

func (con *etcdConenctor) loadServerData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := con.cli.Put(ctx, "ovsdb/_Server/Database/", "sample_value")
	if err!= nil{
		return err
	}
	cancel()
}
