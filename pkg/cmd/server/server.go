package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/metrics"
	"io/ioutil"
	"k8s.io/klog/v2"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

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
	loadServerDataFlag = flag.Bool("load-server-data", false, "load-server-data")
	pidfile            = flag.String("pid-file", "", "Name of file that will hold the pid")
)

var GitCommit string

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()

	klog.V(3).Infof("Start the ovsdb-etcd server \n the last commit: %s\n arguments:\n"+
		"\ttcpAddress: %s\n\tunixAddressress: %s\n\tetcdMembersress: %s\n\tschemaBasedir: %s\n\tmaxTasks: %d\n"+
		"\tdatabasePrefix: %s\n\tserviceName: %s\n\tschemaFile: %s\n\tloadServerData: %v\n\tpid_file: %s\n",
		GitCommit, *tcpAddress, *unixAddress, *etcdMembers, *schemaBasedir, *maxTasks, *databasePrefix, *serviceName,
		*schemaFile, *loadServerDataFlag, *pidfile)

	if len(*tcpAddress) == 0 && len(*unixAddress) == 0 {
		klog.Fatal("You must provide a network-address (TCP and/or UNIX) to listen on")
	}

	if len(*databasePrefix) == 0 || strings.Contains(*databasePrefix, common.KEY_DELIMETER) {
		klog.Fatal("Illegal databasePrefix %s", *databasePrefix)
	}
	if len(*serviceName) == 0 || strings.Contains(*serviceName, common.KEY_DELIMETER) {
		klog.Fatal("Illegal serviceName %s", *serviceName)
	}

	if *pidfile != "" {
		defer delPidfile(*pidfile)
		if err := setupPIDFile(*pidfile); err != nil {
			klog.Fatal(err)
		}
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

	err = db.AddSchema(path.Join(*schemaBasedir, "_server.ovsschema"))
	if err != nil {
		klog.Fatal(err)
	}

	err = db.AddSchema(path.Join(*schemaBasedir, *schemaFile))
	if err != nil {
		klog.Fatal(err)
	}
	// TODO for development only, will be remove later
	if *loadServerDataFlag {
		err = loadServerData(db.(*ovsdb.DatabaseEtcd))
		if err != nil {
			klog.Fatal(err)
		}
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

	loop := func(lst net.Listener) error {
		for {
			conn, err := lst.Accept()
			conn = ConnWrapper{intConn: conn}
			if err != nil {
				klog.V(5).Infof("connection from %v accept error %v  IsErrClosing %v", conn.RemoteAddr(), err, channel.IsErrClosing(err))
				if channel.IsErrClosing(err) {
					err = nil
				} else {
					klog.Infof("Error accepting new connection: %v", err)
				}
				return err
			}
			ch := channel.RawJSON(conn, conn)
			go func() {
				tctx, cancel := context.WithCancel(context.Background())
				handler := ovsdb.NewHandler(tctx, db, cli)
				klog.V(5).Infof("new connection from %v", conn.RemoteAddr())
				assigner := createServicesMap(service, handler)
				srv := jrpc2.NewServer(assigner, servOptions)
				handler.SetConnection(srv, conn)
				srv.Start(ch)
				stat := srv.WaitStatus()
				klog.V(5).Infof("connection from %v, stopped %v closed %v, success %v, err %v", conn.RemoteAddr(), stat.Stopped(), stat.Closed(), stat.Success(), stat.Err)
				if stat.Err != nil {
					klog.Infof("Server exit: %v", stat.Err)
				}
				handler.Cleanup()
				cancel()
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

// we pass handlerMap by value, so the function gets a proprietary copy of it.
func createServicesMap(sharedService *ovsdb.Service, clientHandler *ovsdb.Handler) *handler.Map {
	handlerMap := make(handler.Map)
	handlerMap["list_dbs"] = handler.New(sharedService.ListDbs)
	handlerMap["get_schema"] = handler.New(sharedService.GetSchema)
	handlerMap["get_server_id"] = handler.New(sharedService.GetServerId)
	handlerMap["convert"] = handler.New(sharedService.Convert)

	handlerMap["transact"] = handler.New(clientHandler.Transact)
	handlerMap["cancel"] = handler.New(clientHandler.Cancel)
	handlerMap["monitor"] = handler.New(clientHandler.Monitor)
	handlerMap["monitor_cancel"] = handler.New(clientHandler.MonitorCancel)
	handlerMap["lock"] = handler.New(clientHandler.Lock)
	handlerMap["steal"] = handler.New(clientHandler.Steal)
	handlerMap["unlock"] = handler.New(clientHandler.Unlock)
	handlerMap["monitor_cond"] = handler.New(clientHandler.MonitorCond)
	handlerMap["monitor_cond_since"] = handler.New(clientHandler.MonitorCondSince)
	handlerMap["monitor_cond_change"] = handler.New(clientHandler.MonitorCondChange)
	handlerMap["set_db_change_aware"] = handler.New(clientHandler.SetDbChangeAware)
	handlerMap["echo"] = handler.New(clientHandler.Echo)
	return &handlerMap
}

func delPidfile(pidfile string) {
	if pidfile != "" {
		if _, err := os.Stat(pidfile); err == nil {
			if err := os.Remove(pidfile); err != nil {
				klog.Errorf("%s delete failed: %v", pidfile, err)
			}
		}
	}
}

func setupPIDFile(pidfile string) error {
	// need to test if already there
	_, err := os.Stat(pidfile)

	// Create if it doesn't exist, else exit with error
	if os.IsNotExist(err) {
		if err := ioutil.WriteFile(pidfile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
			klog.Errorf("Failed to write pidfile %s (%v). Ignoring..", pidfile, err)
		}
	} else {
		// get the pid and see if it exists
		pid, err := ioutil.ReadFile(pidfile)
		if err != nil {
			return fmt.Errorf("pidfile %s exists but can't be read: %v", pidfile, err)
		}
		_, err1 := os.Stat("/proc/" + string(pid[:]) + "/cmdline")
		if os.IsNotExist(err1) {
			// Left over pid from dead process
			if err := ioutil.WriteFile(pidfile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
				klog.Errorf("Failed to write pidfile %s (%v). Ignoring..", pidfile, err)
			}
		} else {
			return fmt.Errorf("pidfile %s exists and ovnkube is running", pidfile)
		}
	}

	return nil
}

// temporary for development purpose wrapper
type ConnWrapper struct {
	intConn net.Conn
}

func (cw ConnWrapper) Read(b []byte) (n int, err error) {
	n, err = cw.intConn.Read(b)
	klog.V(7).Infof("read from %v, i= %d err=%v\n ", cw.intConn.RemoteAddr(), n, err)
	return
}

func (cw ConnWrapper) Write(b []byte) (n int, err error) {
	n, err = cw.intConn.Write(b)
	klog.V(7).Infof("write to %v, i= %d err=%v\n ", cw.intConn.RemoteAddr(), n, err)
	return
}

func (cw ConnWrapper) Close() error {
	klog.V(5).Infof("connection from %v called close", cw.intConn.RemoteAddr())
	debug.PrintStack()
	return cw.intConn.Close()
}

func (cw ConnWrapper) LocalAddr() net.Addr {
	return cw.intConn.LocalAddr()
}

func (cw ConnWrapper) RemoteAddr() net.Addr {
	return cw.intConn.RemoteAddr()
}

func (cw ConnWrapper) SetDeadline(t time.Time) error {
	return cw.intConn.SetDeadline(t)
}

func (cw ConnWrapper) SetReadDeadline(t time.Time) error {
	return cw.intConn.SetReadDeadline(t)
}

func (cw ConnWrapper) SetWriteDeadline(t time.Time) error {
	return cw.intConn.SetWriteDeadline(t)
}
