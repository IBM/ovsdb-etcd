package ovsdb

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"time"
)

type DBServer struct {
	cli *clientv3.Client
	schemas map[string]string
}

func NewDBServer( endpoints []string) (*DBServer, error) {
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
	return &DBServer{cli: cli, schemas:make(map[string]string)}, nil
}

func (con *DBServer)AddSchema(schemaName, schemaFile string) error {
	data, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	con.schemas[schemaName] =  string(data)
	return nil
}

func (con *DBServer) LoadServerData() error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/name", "_Server")
	if err!= nil{
		return err
	}
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/model", "standalone")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/connected", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/leader", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b85045f3-78d1-4d52-8831-cbac1f6a86b8/initial/schema", con.schemas["_Server"])

	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/name", "OVN_Northbound")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/model", "standalone")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/connected", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/leader", "true")
	_, err = con.cli.Put(ctx, "ovsdb/_Server/Database/b828af52-6cab-4b46-9870-e4e80e033aad/initial/schema", con.schemas["OVN_Northbound"])
	cancel()
	return err
}

func (con *DBServer) GetData(prefix string) (*clientv3.GetResponse, error) {
	fmt.Printf("GetData " + prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	resp, err := con.cli.Get(ctx, prefix,clientv3.WithFromKey())
	cancel()
	if err != nil {
		return nil, err
	}
	fmt.Printf(" GetDatatype %T \n", resp.Kvs)
	for k, v := range resp.Kvs {
		fmt.Printf("GetData k %v, v %v\n", k, v)
	}
	return resp, err
}
