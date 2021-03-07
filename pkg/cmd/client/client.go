package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"k8s.io/klog/v2"
)

var serverAddr = flag.String("server", "", "Server address")

func list_dbs(ctx context.Context, cli *jrpc2.Client) (result []string, err error) {
	err = cli.CallResult(ctx, "list_dbs", nil, &result)
	return
}

func echo(ctx context.Context, cli *jrpc2.Client) (result []interface{}, err error) {
	err = cli.CallResult(ctx, "echo", []string{"ech0", "echo32"}, &result)
	return
}

func get_server_id(ctx context.Context, cli *jrpc2.Client) (result interface{}, err error) {
	err = cli.CallResult(ctx, "get_server_id", nil, &result)
	return
}

func lock(ctx context.Context, cli *jrpc2.Client, id string) (result interface{}, err error) {
	err = cli.CallResult(ctx, "lock", []string{id}, &result)
	return
}

func unlock(ctx context.Context, cli *jrpc2.Client, id string) (result interface{}, err error) {
	err = cli.CallResult(ctx, "unlock", []string{id}, &result)
	return
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	if *serverAddr == "" {
		klog.Fatal("You must provide -server address to connect to")
	}

	conn, err := net.Dial(jrpc2.Network(*serverAddr), *serverAddr)
	if err != nil {
		klog.Fatalf("Dial %q: %v", *serverAddr, err)
	}
	klog.Infof("Connected to %v", conn.RemoteAddr())

	// Start up the client, and enable logging to stderr.
	cli := jrpc2.NewClient(channel.RawJSON(conn, conn), &jrpc2.ClientOptions{
		OnNotify: func(req *jrpc2.Request) {
			var params json.RawMessage
			req.UnmarshalParams(&params)
			klog.Infof("[server push] Method %q params %#q", req.Method(), string(params))
		},
		AllowV1: true,
	})
	defer cli.Close()
	ctx := context.Background()

	klog.Info("\n-- Sending some individual requests...")

	if dbs, err := list_dbs(ctx, cli); err != nil {
		klog.Fatalln("Ovsdb.List_dbs:", err)
	} else {
		klog.Info("Ovsdb.List_dbs result=%v", dbs)
	}

	if echo, err := echo(ctx, cli); err != nil {
		klog.Fatalln("Ovsdb.Echo:", err)
	} else {
		klog.Info("Ovsdb.Echo result=%v", echo)
	}

	if uuid, err := get_server_id(ctx, cli); err != nil {
		klog.Fatalln("Get_server_id:", err)
	} else {
		klog.Info("Get_server_id result=%v", uuid)
	}

	if lock, err := lock(ctx, cli, "test1"); err != nil {
		klog.Fatalln("lock:", err)
	} else {
		klog.Info("lock result=%v", lock)
	}
	if lock, err := unlock(ctx, cli, "test1"); err != nil {
		klog.Fatalf("unlock: %v", err)
	} else {
		klog.Info("unlock result=%v", lock)
	}

}
