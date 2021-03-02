package main

import (
	"flag"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/metrics"
	"github.com/creachadair/jrpc2/server"
	"log"
	"net"
	"os"
	"runtime"

	"github.com/creachadair/jrpc2"
	"github.com/roytman/ovsdb-etcd/pkg/ovsdb"

)

const UNIX_SOCKET = "/tmp/ovsdb-etcd.sock"
const ETCD_LOCALHOST = "localhost:2379"

var (
	tcpAddress  = flag.String("tcp-address", "", "TCP service address")
	unixAddress = flag.String("unix-address", "", "UNIX service address")
	etcdMembers = flag.String("etcd-members", ETCD_LOCALHOST, "TCP service address")
	maxTasks = flag.Int("max", 1, "Maximum concurrent tasks")
)


func main() {

	flag.Parse()
	if len(*tcpAddress) == 0 && len(*unixAddress) == 0 {
		log.Fatal("You must provide a network-address (TCP and/or UNIX) to listen on")
	}

	dbServ, err := ovsdb.NewDBServer([]string{"localhost:2379"})
	if err != nil {
		log.Fatal(err)
	}

	err = dbServ.AddSchema("_Server", "./json/_server.ovsschema")
	if err != nil {
		log.Fatal(err)
	}
	err = dbServ.AddSchema("OVN_Northbound", "./json/ovn-nb.ovsschema")
	if err != nil {
		log.Fatal(err)
	}
	err = dbServ.LoadServerData()
	if err != nil {
		log.Fatal(err)
	}

	// create and init OVSD service
	// Bind the methods of the math type to an assigner.
	ovsdbServ := ovsdb.NewService(dbServ)
	mux := handler.ServiceMap{
		"Ovsdb": handler.NewService(ovsdbServ),
	}



	if len(*tcpAddress) > 0 {
		lst, err := net.Listen(jrpc2.Network(*tcpAddress), *tcpAddress)
		if err != nil {
			log.Fatalln("Listen:", err)
		}
		log.Printf("Listening at %v...", lst.Addr())

		servOptions := &jrpc2.ServerOptions{
			Logger:      log.New(os.Stderr, "[TCP.Server] ", log.LstdFlags|log.Lshortfile),
			Concurrency: *maxTasks,
			Metrics:     metrics.New(),
			AllowPush:   true,
			AllowV1:     true,
		}
		server.Loop(lst, server.NewStatic(mux), &server.LoopOptions{ServerOptions: servOptions})
	}
	if runtime.GOOS == "linux" && len(*unixAddress) > 0 {
		if err := os.RemoveAll(*unixAddress); err != nil {
			log.Fatal(err)
		}
		lst, err := net.Listen(jrpc2.Network(*unixAddress), *unixAddress)
		if err != nil {
			log.Fatalln("Listen:", err)
		}
		log.Printf("Listening at %v...", lst.Addr())

		servOptions := &jrpc2.ServerOptions{
			Logger:      log.New(os.Stderr, "[UNIX.Server] ", log.LstdFlags|log.Lshortfile),
			Concurrency: *maxTasks,
			Metrics:     metrics.New(),
			AllowPush:   true,
			AllowV1:     true,
		}
		server.Loop(lst, server.NewStatic(mux), &server.LoopOptions{ServerOptions: servOptions})
	}
}
