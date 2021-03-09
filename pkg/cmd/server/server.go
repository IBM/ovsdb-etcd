package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/metrics"
	"github.com/creachadair/jrpc2/server"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/ovsdb"
)

const UNIX_SOCKET = "/tmp/ovsdb-etcd.sock"
const ETCD_LOCALHOST = "localhost:2379"

var (
	tcpAddress  = flag.String("tcp-address", "", "TCP service address")
	unixAddress = flag.String("unix-address", "", "UNIX service address")
	etcdMembers = flag.String("etcd-members", ETCD_LOCALHOST, "ETCD service addresses, separated by ',' ")
	maxTasks    = flag.Int("max", 1, "Maximum concurrent tasks")
)

var methodsMap = map[string]string{
	"ListDbs":           "list_dbs",
	"GetSchema":         "get_schema",
	"Transact":          "transact",
	"Cancel":            "cancel",
	"Monitor":           "monitor",
	"MonitorCancel":     "monitor_cancel",
	"Lock":              "lock",
	"Steal":             "steal",
	"Unlock":            "unlock",
	"Echo":              "echo",
	"MonitorCond":       "monitor_cond",
	"MonitorCondChange": "monitor_cond_change",
	"MonitorCondSince":  "monitor_cond_since",
	"GetServerId":       "get_server_id",
	"SetDbChangeAware":  "set_db_change_aware",
	"Convert":           "convert",
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	if len(*tcpAddress) == 0 && len(*unixAddress) == 0 {
		klog.Fatal("You must provide a network-address (TCP and/or UNIX) to listen on")
	}

	if len(*etcdMembers) == 0 {
		klog.Fatal("Wrong ETCD members list", etcdMembers)
	}
	etcdServers := strings.Split(*etcdMembers, ",")
	dbServ, err := ovsdb.NewDBServer(etcdServers)
	if err != nil {
		klog.Fatal(err)
	}

	// For development only
	err = dbServ.AddSchema("_Server", "./schemas/_server.ovsschema")
	if err != nil {
		klog.Fatal(err)
	}
	err = dbServ.AddSchema("OVN_Northbound", "./schemas/ovn-nb.ovsschema")
	if err != nil {
		klog.Fatal(err)
	}
	err = dbServ.LoadServerData()
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
	ovsdbServ := ovsdb.NewService(dbServ)
	mux := methodsToService(ovsdbServ)
	srvFunc := server.NewStatic(mux)

	// a WaitGroup for the goroutines to tell us they've stopped
	wg := sync.WaitGroup{}

	if len(*tcpAddress) > 0 {
		lst, err := net.Listen(jrpc2.Network(*tcpAddress), *tcpAddress)
		if err != nil {
			klog.Fatalln("Listen:", err)
		}
		klog.Infof("Listening at %v...", lst.Addr())
		//servOptions.Logger = log.New(os.Stderr, "[TCP.Server] ", log.LstdFlags|log.Lshortfile)

		go serverLoop(ctx, lst, srvFunc, servOptions, &wg)
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
		go serverLoop(ctx, lst, srvFunc, servOptions, &wg)
	}

	select {
	case s := <-exitCh:
		klog.Infof("Received signal %s. Shutting down", s)
		cancel()
	case <-ctx.Done():
	}

}

// the method is similar to the handler.NewService, but adopted to the ovsdb implementation.
func methodsToService(ovsdb ovsdb.OVSDB) handler.Map {
	out := make(handler.Map)
	val := reflect.ValueOf(ovsdb)
	// This considers only exported methods, as desired.
	for method, ovsdbMethod := range methodsMap {
		fn := val.MethodByName(method)
		if !fn.IsValid() {
			klog.Errorf(" OVSDB doesn't contain method %s", method)
			continue
		}
		v := handler.New(fn.Interface())
		out[ovsdbMethod] = v
	}
	return out
}

func serverLoop(ctx context.Context, lst net.Listener, newService func() server.Service, serverOpts *jrpc2.ServerOptions, wg *sync.WaitGroup) error {
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
			svc := newService()
			assigner, err := svc.Assigner()
			if err != nil {
				klog.Errorf("Service initialization failed: %v", err)
				return
			}
			srv := jrpc2.NewServer(assigner, serverOpts).Start(ch)
			// create and init OVSD service
			// Bind the methods of the math type to an assigner.

			stat := srv.WaitStatus()
			svc.Finish(stat)
			if stat.Err != nil {
				klog.Infof("Server exit: %v", stat.Err)
			}
		}()
	}
}
