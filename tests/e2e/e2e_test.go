package e2e_test

import (
	"context"
	"encoding/json"
	"net"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"k8s.io/klog/v2"
)

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

func dial(serverAddr string) (cli *jrpc2.Client, ctx context.Context) {
	conn, err := net.Dial(jrpc2.Network(serverAddr), serverAddr)
	if err != nil {
		klog.Fatalf("Dial %q: %v", serverAddr, err)
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
	return
}

var _ = Describe("e2e", func() {
	var _ = Describe("e2e ovsdb-etcd", func() {
		DescribeTable("ovsdb-etcd operations",
			func(serverAddr string) {
				cli, ctx := dial(serverAddr)
				Context("should be able to show the dbs list", func() {
					dbs, err := listDbs(ctx, cli)
					Expect(err).ShouldNot(HaveOccurred())
					klog.Infof("listDbs result=%v", dbs)
				})
				Context("should be able to echo a messages", func() {
					echo, err := echo(ctx, cli)
					Expect(err).ShouldNot(HaveOccurred())
					klog.Infof("echo result=%v", echo)
				})
				Context("should be able to echo a messages", func() {
					echo, err := echo(ctx, cli)
					Expect(err).ShouldNot(HaveOccurred())
					klog.Infof("echo result=%v", echo)
				})
				Context("should be able to retrieve the server ID", func() {
					uuid, err := getServerId(ctx, cli)
					Expect(err).ShouldNot(HaveOccurred())
					klog.Infof("getServerId result=%v", uuid)
				})
				Context("should be able to lock", func() {
					lock, err := lock(ctx, cli, "test1")
					Expect(err).ShouldNot(HaveOccurred())
					klog.Infof("lock result=%v", lock)
				})
				Context("should be able to unlock", func() {
					lock, err := unlock(ctx, cli, "test1")
					Expect(err).ShouldNot(HaveOccurred())
					klog.Infof("unlock result=%v", lock)
				})
				cli.Close()
			},
			Entry("ovn North-bound", "127.0.0.1:6641"),
			//Entry("ovn South-bound", "127.0.0.1:6642"),
		)
	})
})
