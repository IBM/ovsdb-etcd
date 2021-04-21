package e2e_test

import (
	"context"
	"encoding/json"
	"flag"
	"net"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	. "github.com/onsi/ginkgo"
	"k8s.io/klog/v2"
)

var serverAddr = flag.String("server", "127.0.0.1:12345", "Server address")
var cli *jrpc2.Client
var ctx context.Context

func listDbs(ctx context.Context, cli *jrpc2.Client) (result []string, err error) {
	err = cli.CallResult(ctx, "list_dbs", nil, &result)
	return
}
func echo(ctx context.Context, cli *jrpc2.Client) (result []interface{}, err error) {
	err = cli.CallResult(ctx, "echo", []string{"ech0", "echo32"}, &result)
	return
}

func getServerId(ctx context.Context, cli *jrpc2.Client) (result interface{}, err error) {
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

var _ = BeforeSuite(func() {
	conn, err := net.Dial(jrpc2.Network(*serverAddr), *serverAddr)
	if err != nil {
		klog.Fatalf("Dial %q: %v", *serverAddr, err)
	}
	klog.Infof("Connected to %v", conn.RemoteAddr())
	// Start up the client, and enable logging to stderr.
	cli = jrpc2.NewClient(channel.RawJSON(conn, conn), &jrpc2.ClientOptions{
		OnNotify: func(req *jrpc2.Request) {
			var params json.RawMessage
			req.UnmarshalParams(&params)
			klog.Infof("[server push] Method %q params %#q", req.Method(), string(params))
		},
		AllowV1: true,
	})
	ctx = context.Background()
})

var _ = AfterSuite(func() {
	cli.Close()
})

var _ = Describe("E2e", func() {
	Describe("basic db Checks", func() {
		It("should be able to show the dbs list", func() {
			if dbs, err := listDbs(ctx, cli); err != nil {
				klog.Fatalln("listDbs Error:", err)
			} else {
				klog.Infof("listDbs result=%v", dbs)
			}
			return
		})
		It("should be able to echo a messeges", func() {
			if echo, err := echo(ctx, cli); err != nil {
				klog.Fatalln("echo:", err)
			} else {
				klog.Infof("echo result=%v", echo)
			}
			return
		})
		It("should be able to retrive the server ID", func() {
			if uuid, err := getServerId(ctx, cli); err != nil {
				klog.Fatalln("getServerId:", err)
			} else {
				klog.Infof("getServerId result=%v", uuid)
			}
			return
		})
		It("should be able to lock", func() {
			if lock, err := lock(ctx, cli, "test1"); err != nil {
				klog.Fatalln("lock:", err)
			} else {
				klog.Infof("lock result=%v", lock)
			}
			return
		})
		It("should be able to unlock", func() {
			if lock, err := unlock(ctx, cli, "test1"); err != nil {
				klog.Fatalf("unlock: %v", err)
			} else {
				klog.Infof("unlock result=%v", lock)
			}
			return
		})
	})
})
