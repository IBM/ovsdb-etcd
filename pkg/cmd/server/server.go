package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/metrics"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsdb"
)

const UNIX_SOCKET = "/tmp/ovsdb-etcd.sock"
const ETCD_LOCALHOST = "localhost:2379"

var (
	tcpAddress         = flag.String("tcp-address", "", "TCP service address")
	unixAddress        = flag.String("unix-address", "", "UNIX service address")
	etcdMembers        = flag.String("etcd-members", ETCD_LOCALHOST, "ETCD service addresses, separated by ',' ")
	schemaBasedir      = flag.String("schema-basedir", ".", "Schema base dir")
	maxTasks           = flag.Int("max", 1, "Maximum concurrent tasks")
	databasePrefix     = flag.String("database-prefix", "ovsdb", "Database prefix")
	serviceName        = flag.String("service-name", "", "Deployment service name, e.g. 'nbdb' or 'sbdb'")
	schemaFile         = flag.String("schema-file", "", "schema-file")
	schemaName         = flag.String("schema-name", "", "schema-name")
	loadServerDataFlag = flag.Bool("load-server-data", false, "load-server-data")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	//TODO: for development purposes only. will be remove later
	fmt.Println("start the ovsdb-etcd server with the following arguments:")
	fmt.Printf("\ttcpAddress:%s\n\tunixAddressress:%s\n\tetcdMembersress:%s\n\tschemaBasedir:%s\n\tmaxTasks:%d\n\tdatabasePrefix:%s\n\tserviceName:%s\n\tschemaFile:%s\n\tschemaName:%s\n\tloadServerData:%v\n",
		*tcpAddress, *unixAddress, *etcdMembers, *schemaBasedir, *maxTasks, *databasePrefix, *serviceName, *schemaFile, *schemaName, *loadServerDataFlag)

	if len(*tcpAddress) == 0 && len(*unixAddress) == 0 {
		klog.Fatal("You must provide a network-address (TCP and/or UNIX) to listen on")
	}

	if len(*databasePrefix) == 0 || strings.Contains(*databasePrefix, common.KEY_DELIMETER) {
		klog.Fatal("Illegal databasePrefix %s", *databasePrefix)
	}
	if len(*serviceName) == 0 || strings.Contains(*serviceName, common.KEY_DELIMETER) {
		klog.Fatal("Illegal serviceName %s", *serviceName)
	}

	// several OVSDB deployments can share the same etcd, but for rest of the work, we don't have to separate
	// databasePrefix and serviceName.
	common.SetPrefix(*databasePrefix + common.KEY_DELIMETER + *serviceName)

	if len(*etcdMembers) == 0 {
		klog.Fatal("Wrong ETCD members list", etcdMembers)
	}
	etcdServers := strings.Split(*etcdMembers, ",")

	cli, err := ovsdb.NewEtcdClient(etcdServers)
	if err != nil {
		klog.Fatal(err)
	}
	defer cli.Close()

	db, _ := ovsdb.NewDatabaseEtcd(cli)

	err = db.AddSchema("_Server", *schemaBasedir+"/_server.ovsschema")
	if err != nil {
		klog.Fatal(err)
	}
	err = db.AddSchema(*schemaName, path.Join(*schemaBasedir, *schemaFile))
	if err != nil {
		klog.Fatal(err)
	}
	err = loadServerData(db.(*ovsdb.DatabaseEtcd))
	if err != nil {
		klog.Fatal(err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	exitCh := make(chan os.Signal, 1)
	signal.Notify(exitCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	defer func() {
		signal.Stop(exitCh)
		cancel()
	}()

	servOptions := &jrpc2.ServerOptions{
		Concurrency: *maxTasks,
		Metrics:     metrics.New(),
		AllowPush:   true,
		AllowV1:     true,
	}
	service := ovsdb.NewService(db)
	globServiceMap := createServiceMap(service)
	wg := sync.WaitGroup{}

	loop := func(lst net.Listener) error {
		for {
			conn, err := lst.Accept()
			if err != nil {
				if channel.IsErrClosing(err) {
					err = nil
				} else {
					klog.Infof("Error accepting new connection: %v", err)
				}
				wg.Wait()
				return err
			}
			ch := channel.RawJSON(conn, conn)
			wg.Add(1)
			go func() {
				defer wg.Done()
				tctx, cancel := context.WithCancel(context.Background())
				handler := ovsdb.NewHandler(tctx, db, cli)
				assigner := addClientHandlers(*globServiceMap, handler)
				srv := jrpc2.NewServer(assigner, servOptions)
				handler.SetConnection(srv)
				srv.Start(ch)
				go func() {
					defer cancel()
					srv.Wait()
					handler.Cleanup()
				}()
				stat := srv.WaitStatus()
				if stat.Err != nil {
					klog.Infof("Server exit: %v", stat.Err)
				}
			}()
		}
	}
	if len(*tcpAddress) > 0 {
		lst, err := net.Listen(jrpc2.Network(*tcpAddress), *tcpAddress)
		if err != nil {
			klog.Fatalln("Listen:", err)
		}
		klog.Infof("Listening at %v...", lst.Addr())
		//servOptions.Logger = log.New(os.Stderr, "[TCP.Server] ", log.LstdFlags|log.Lshortfile)

		go loop(lst)
	}
	if runtime.GOOS == "linux" && len(*unixAddress) > 0 {
		if err := os.RemoveAll(*unixAddress); err != nil {
			klog.Fatal(err)
		}
		lst, err := net.Listen(jrpc2.Network(*unixAddress), *unixAddress)
		if err != nil {
			klog.Fatalln("Listen:", err)
		}
		klog.Infof("Listening at %v...", lst.Addr())
		//servOptions.Logger = log.New(os.Stderr, "[UNIX.Server] ", log.LstdFlags|log.Lshortfile)
		go loop(lst)
	}
	select {
	case s := <-exitCh:
		klog.Infof("Received signal %s. Shutting down", s)
		cancel()
	case <-ctx.Done():
	}

}

func createServiceMap(ovsdb *ovsdb.Service) *handler.Map {
	out := make(handler.Map)
	out["list_dbs"] = handler.New(ovsdb.ListDbs)
	out["get_schema"] = handler.New(ovsdb.GetSchema)
	out["get_server_id"] = handler.New(ovsdb.GetServerId)
	out["echo"] = handler.New(ovsdb.Echo)
	out["convert"] = handler.New(ovsdb.Convert)
	return &out
}

// we pass handlerMap by value, so the function gets a proprietary copy of it.
func addClientHandlers(handlerMap handler.Map, ch *ovsdb.Handler) *handler.Map {
	handlerMap["transact"] = handler.New(ch.Transact)
	handlerMap["cancel"] = handler.New(ch.Cancel)
	handlerMap["monitor"] = handler.New(ch.Monitor)
	handlerMap["monitor_cancel"] = handler.New(ch.MonitorCancel)
	handlerMap["lock"] = handler.New(ch.Lock)
	handlerMap["steal"] = handler.New(ch.Steal)
	handlerMap["unlock"] = handler.New(ch.Unlock)
	handlerMap["monitor_cond"] = handler.New(ch.MonitorCond)
	handlerMap["monitor_cond_since"] = handler.New(ch.MonitorCondSince)
	handlerMap["monitor_cond_change"] = handler.New(ch.MonitorCondChange)
	handlerMap["set_db_change_aware"] = handler.New(ch.SetDbChangeAware)
	return &handlerMap
}
