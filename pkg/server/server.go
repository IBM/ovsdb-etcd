package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime"

	//	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/roytman/ovsdb-etcd/pkg/ovsdb"
)

const UNIX_SOCKET = "/tmp/ovsdb-etcd.sock"


func main() {
	// create and init OVSD service
	ovsdbServ := ovsdb.NewService()

	err := ovsdb.AddSchema(ovsdbServ, "_Server", "./json/_server.ovsschema")
	if err != nil {
		log.Fatal(err)
	}
	err = ovsdb.AddSchema(ovsdbServ,"OVN_Northbound", "./json/ovn-nb.ovsschema")
	if err != nil {
		log.Fatal(err)
	}

	/*
	data, err := ioutil.ReadFile("./pkg/server/cond.json")
	if err != nil {
		log.Fatal(err)
	}
	*/

	//cond = string(data)
	/*var cli *clientv3.Client
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	fmt.Printf("etcd client is connected")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = cli.Put(ctx, "sample_key", "sample_value")
	cancel()
	if err != nil {
		log.Fatal(err)
	}*/

	ms := rpc.NewServer()
	ms.RegisterName("ovsdb", ovsdbServ)


	addyr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:12345")
	if err != nil {
		log.Fatal(err)
	}
	inbound, err := net.ListenTCP("tcp", addyr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ovsdb-etcd server is bound on %v\n", addyr)

	if runtime.GOOS == "linux" {
		if err := os.RemoveAll(UNIX_SOCKET); err != nil {
			log.Fatal(err)
		}
		inbound2, err := net.Listen("unix", UNIX_SOCKET)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("ovsdb-etcd server is bound on %s\n", UNIX_SOCKET)
		go func() {
			for {
				conn, err := inbound2.Accept()
				if err != nil {
					continue
				}
				ms.ServeCodec(ovsdb.NewCodec(conn))
			}
		}()
	}
	for {
		conn, err := inbound.Accept()
		if err != nil {
			continue
		}
		ms.ServeCodec(ovsdb.NewCodec(conn))
	}
}
