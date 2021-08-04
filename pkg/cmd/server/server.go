package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/metrics"
	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"

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
	maxTasks           = flag.Int("max", 10, "Maximum concurrent tasks")
	databasePrefix     = flag.String("database-prefix", "ovsdb", "Database prefix")
	serviceName        = flag.String("service-name", "", "Deployment service name, e.g. 'nbdb' or 'sbdb'")
	schemaFile         = flag.String("schema-file", "", "schema-file")
	loadServerDataFlag = flag.Bool("load-server-data", false, "load-server-data")
	pidFile            = flag.String("pid-file", "", "Name of file that will hold the pid")
	cpuProfile         = flag.String("cpu-profile", "", "write cpu profile to file")
)

var GitCommit string

var log logr.Logger

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()
	log = klogr.New()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Error(err, "failed to create cpu profile")
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.V(3).Info("start the ovsdb-etcd server", "git-commit", GitCommit,
		"tcp-address", tcpAddress, "unix-address", unixAddress, "etcd-members",
		etcdMembers, "schema-basedir", schemaBasedir, "max-tasks", maxTasks,
		"database-prefix", databasePrefix, "service-name", serviceName,
		"schema-file", schemaFile, "load-server-data-flag", loadServerDataFlag,
		"pid-file", pidFile, "cpu-profile", cpuProfile)

	if len(*tcpAddress) == 0 && len(*unixAddress) == 0 {
		log.Info("You must provide a network-address (TCP and/or UNIX) to listen on")
		os.Exit(1)
	}

	if len(*databasePrefix) == 0 || strings.Contains(*databasePrefix, common.KEY_DELIMETER) {
		log.Info("Illegal databasePrefix %s", *databasePrefix)
		os.Exit(1)
	}
	if len(*serviceName) == 0 || strings.Contains(*serviceName, common.KEY_DELIMETER) {
		log.Info("Illegal serviceName %s", *serviceName)
		os.Exit(1)
	}

	if *pidFile != "" {
		defer delPIDFile(*pidFile)
		if err := setupPIDFile(*pidFile); err != nil {
			klog.Fatal(err)
		}
	}

	// several OVSDB deployments can share the same etcd, but for rest of the work, we don't have to separate
	// databasePrefix and serviceName.
	common.SetPrefix(*databasePrefix + common.KEY_DELIMETER + *serviceName)

	if len(*etcdMembers) == 0 {
		log.Info("Wrong ETCD members list", etcdMembers)
		os.Exit(1)
	}
	etcdServers := strings.Split(*etcdMembers, ",")

	cli, err := ovsdb.NewEtcdClient(etcdServers)
	if err != nil {
		log.Error(err, "failed creating an etcd client")
		os.Exit(1)
	}
	defer cli.Close()

	db, _ := ovsdb.NewDatabaseEtcd(cli, log)

	err = db.AddSchema(path.Join(*schemaBasedir, "_server.ovsschema"))
	if err != nil {
		log.Error(err, "failed to add schema")
		os.Exit(1)
	}

	err = db.AddSchema(path.Join(*schemaBasedir, *schemaFile))
	if err != nil {
		log.Error(err, "failed to add schema")
		os.Exit(1)
	}

	// TODO for development only, will be remove later
	if *loadServerDataFlag {
		err = loadServerData(db.(*ovsdb.DatabaseEtcd))
		if err != nil {
			log.Error(err, "failed to load server data")
			os.Exit(1)
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
			conn = ConnWrapper{IntConn: conn}
			if err != nil {
				log.V(5).Info("connection", "from", conn.RemoteAddr(), "error", err, "is-closing", channel.IsErrClosing(err))
				if channel.IsErrClosing(err) {
					err = nil
				} else {
					log.Error(err, "failed accepting new connection")
				}
				return err
			}
			ch := channel.RawJSON(conn, conn)
			go func() {
				tctx, cancel := context.WithCancel(context.Background())
				handler := ovsdb.NewHandler(tctx, db, cli, log)
				log.V(5).Info("new connection", "from", conn.RemoteAddr())
				assigner := createServicesMap(service, handler)
				srv := jrpc2.NewServer(assigner, servOptions)
				handler.SetConnection(srv, conn)
				srv.Start(ch)
				stat := srv.WaitStatus()
				log.V(5).Info("connection", "from", conn.RemoteAddr(), "stopped", stat.Stopped(), "closed", stat.Closed(), "success", stat.Success(), "err", stat.Err)
				if stat.Err != nil {
					log.Error(err, "Server exit")
				}
				handler.Cleanup()
				cancel()
			}()
		}
	}
	if len(*tcpAddress) > 0 {
		lst, err := net.Listen(jrpc2.Network(*tcpAddress), *tcpAddress)
		if err != nil {
			log.Error(err, "failed listen")
		}
		log.Info("listening", "on", lst.Addr())
		//servOptions.Logger = log.New(os.Stderr, "[TCP.Server] ", log.LstdFlags|log.Lshortfile)

		go loop(lst)
	}
	if runtime.GOOS == "linux" && len(*unixAddress) > 0 {
		if err := os.RemoveAll(*unixAddress); err != nil {
			log.Error(err, "failed to remove all address")
			os.Exit(1)
		}
		lst, err := net.Listen(jrpc2.Network(*unixAddress), *unixAddress)
		if err != nil {
			log.Error(err, "failed listen")
			os.Exit(1)
		}
		log.Info("listening", "on", lst.Addr())
		//servOptions.Logger = log.New(os.Stderr, "[UNIX.Server] ", log.LstdFlags|log.Lshortfile)
		go loop(lst)
	}
	select {
	case s := <-exitCh:
		log.Info("Received signal shutting down", "signal", s)
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

func delPIDFile(pidFile string) {
	if pidFile != "" {
		if _, err := os.Stat(pidFile); err == nil {
			if err := os.Remove(pidFile); err != nil {
				log.Error(err, "delete failed", "pidFile", pidFile)
			}
		}
	}
}

func setupPIDFile(pidFile string) error {
	// need to test if already there
	_, err := os.Stat(pidFile)

	// Create if it doesn't exist, else exit with error
	if os.IsNotExist(err) {
		if err := ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
			log.Error(err, "failed to write pidFile. ignoring..", "pidFile", pidFile)
		}
	} else {
		// get the pid and see if it exists
		pid, err := ioutil.ReadFile(pidFile)
		if err != nil {
			return fmt.Errorf("pidFile %s exists but can't be read: %v", pidFile, err)
		}
		_, err1 := os.Stat("/proc/" + string(pid[:]) + "/cmdline")
		if os.IsNotExist(err1) {
			// Left over pid from dead process
			if err := ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
				log.Error(err, "failed to write pidFile. ignoring..", "pidFile", pidFile)
			}
		} else {
			return fmt.Errorf("pidFile %s exists and ovnkube is running", pidFile)
		}
	}

	return nil
}

// temporary for development purpose wrapper
type ConnWrapper struct {
	IntConn net.Conn
}

func (cw ConnWrapper) Read(b []byte) (n int, err error) {
	n, err = cw.IntConn.Read(b)
	log.V(7).Info("read", "from", cw.RemoteAddr(), "bytes", n, "error", err)
	return
}

func (cw ConnWrapper) Write(b []byte) (n int, err error) {
	n, err = cw.IntConn.Write(b)
	log.V(7).Info("write", "from", cw.RemoteAddr(), "bytes", n, "error", err)
	return
}

func (cw ConnWrapper) Close() error {
	log.V(5).Info("close", "from", cw.RemoteAddr())
	return cw.IntConn.Close()
}

func (cw ConnWrapper) LocalAddr() net.Addr {
	return cw.IntConn.LocalAddr()
}

type customUnixAddr struct {
	ClientName string
}

func (c customUnixAddr) String() string {
	return c.ClientName
}
func (c customUnixAddr) Network() string {
	return "unix"
}

func (cw ConnWrapper) RemoteAddr() net.Addr {
	switch cw.IntConn.(type) {
	case *net.UnixConn:
		return customUnixAddr{ClientName: "unix-client"}
	default:
		return cw.IntConn.RemoteAddr()
	}
}

func (cw ConnWrapper) SetDeadline(t time.Time) error {
	return cw.IntConn.SetDeadline(t)
}

func (cw ConnWrapper) SetReadDeadline(t time.Time) error {
	return cw.IntConn.SetReadDeadline(t)
}

func (cw ConnWrapper) SetWriteDeadline(t time.Time) error {
	return cw.IntConn.SetWriteDeadline(t)
}
