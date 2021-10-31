package main

import (
	"context"
	"crypto/tls"
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
	jrpcHandler "github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/metrics"
	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/cmd/ssl_utils"
	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsdb"
)

const ETCD_LOCALHOST = "localhost:2379"

var (
	tcpAddress      = flag.String("tcp-address", "", "TCP service address")
	unixAddress     = flag.String("unix-address", "", "UNIX service address")
	etcdMembers     = flag.String("etcd-members", ETCD_LOCALHOST, "ETCD service addresses, separated by ',' ")
	schemaBasedir   = flag.String("schema-basedir", ".", "Schema base dir")
	maxTasks        = flag.Int("max", 10, "Maximum concurrent tasks")
	databasePrefix  = flag.String("database-prefix", "ovsdb", "Database prefix")
	serviceName     = flag.String("service-name", "", "Deployment service name, e.g. 'nbdb' or 'sbdb'")
	schemaFile      = flag.String("schema-file", "", "schema-file")
	deploymentModel = flag.String("model", "clustered", "deployment model, possible values: clustered, standalone")
	// returned back for backward compatability with executable scripts, wil be removed later
	loadServerDataFlag = flag.Bool("load-server-data", false, "load-server-data")
	pidFile            = flag.String("pid-file", "", "Name of file that will hold the pid")
	cpuProfile         = flag.String("cpu-profile", "", "write cpu profile to file")
	keepAliveTime      = flag.Duration("keepalive-time", -1*time.Second, "keepalive time for the etcd client connection")
	keepAliveTimeout   = flag.Duration("keepalive-timeout", -1*time.Second, "keepalive timeout for the etcd client connection")
	sslPrivateKeyFile  = flag.String("ssl-privkey", "", "path to ssl private key file")
	sslCertificateFile = flag.String("ssl-cert", "", "path to ssl certificate file")
	sslVersion         = ssl_utils.SslVersion
	sslCipherSuite     = ssl_utils.SslCipherSuite
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
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Error(err, "failed to create cpu profile")
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	log.V(3).Info("start the ovsdb-etcd server", "git-commit", GitCommit,
		"tcp-address", tcpAddress, "unix-address", unixAddress, "etcd-members",
		etcdMembers, "schema-basedir", schemaBasedir, "max-tasks", maxTasks,
		"database-prefix", databasePrefix, "service-name", serviceName,
		"schema-file", schemaFile, "load-server-data-flag", loadServerDataFlag,
		"pid-file", pidFile, "cpu-profile", cpuProfile,
		"keepalive-time", keepAliveTime, "keepalive-timeout", keepAliveTimeout,
		"ssl-privkey", sslPrivateKeyFile, "ssl-cert", sslCertificateFile,
	)

	if len(*tcpAddress) == 0 && len(*unixAddress) == 0 {
		log.Info("You must provide a network-address (TCP and/or UNIX) to listen on")
		os.Exit(1)
	}

	if len(*databasePrefix) == 0 || strings.Contains(*databasePrefix, common.KEY_DELIMITER) {
		log.Info("Illegal databasePrefix %s", *databasePrefix)
		os.Exit(1)
	}
	if len(*serviceName) == 0 || strings.Contains(*serviceName, common.KEY_DELIMITER) {
		log.Info("Illegal serviceName %s", *serviceName)
		os.Exit(1)
	}

	if *pidFile != "" {
		defer delPIDFile(*pidFile)
		if err := setupPIDFile(*pidFile); err != nil {
			log.Error(err, "failed to setup PID file")
			os.Exit(1)
		}
	}

	// databasePrefix and serviceName.
	common.SetPrefix(*databasePrefix + common.KEY_DELIMITER + *serviceName)

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

	if len(*etcdMembers) == 0 {
		log.Info("Wrong ETCD members list", etcdMembers)
		os.Exit(1)
	}
	etcdServers := strings.Split(*etcdMembers, ",")
	cli, err := ovsdb.NewEtcdClient(ctx, etcdServers, *keepAliveTime, *keepAliveTimeout)
	if err != nil {
		log.Error(err, "failed creating an etcd client")
		os.Exit(1)
	}
	defer func() {
		err := cli.Close()
		log.Error(err, "cli close")
	}()

	db, _ := ovsdb.NewDatabaseEtcd(cli, *deploymentModel, log)

	err = db.AddSchema(path.Join(*schemaBasedir, "_server.ovsschema"))
	if err != nil {
		log.Error(err, "failed to add _server schema")
		os.Exit(1)
	}

	err = db.AddSchema(path.Join(*schemaBasedir, *schemaFile))
	if err != nil {
		log.Error(err, "failed to add schema", "schema file", *schemaFile)
		os.Exit(1)
	}
	//db.StartLeaderElection()

	servOptions := &jrpc2.ServerOptions{
		Concurrency: *maxTasks,
		Metrics:     metrics.New(),
		AllowPush:   true,
		AllowV1:     true,
	}
	service := ovsdb.NewService(db)

	loop := func(lst net.Listener) {
		for {
			conn, err := lst.Accept()
			if err != nil {
				log.Error(err, "Accept returned")
				os.Exit(1)
			}
			ch := channel.RawJSON(conn, conn)
			go func() {
				ctxt, cancel := context.WithCancel(context.Background())
				handler := ovsdb.NewHandler(ctxt, db, cli, log)
				log.V(5).Info("new connection", "from", conn.RemoteAddr())
				assigner := createServicesMap(service, handler)
				srv := jrpc2.NewServer(assigner, servOptions)
				handler.SetConnection(srv, conn)
				srv.Start(ch)
				stat := srv.WaitStatus()
				log.V(5).Info("connection", "from", conn.RemoteAddr(), "stopped", stat.Stopped(), "closed", stat.Closed(), "success", stat.Success(), "err", stat.Err)
				handler.Cleanup()
				cancel()
			}()
		}
	}
	if len(*tcpAddress) > 0 {
		var lst net.Listener
		var err error
		if len(*sslPrivateKeyFile) > 0 && len(*sslCertificateFile) > 0 {
			log.Info("create listener with HTTPS using:", "SSL private key file:", *sslPrivateKeyFile, ",SSL sslCertificate file:", *sslCertificateFile)
			cer, err := tls.LoadX509KeyPair(*sslCertificateFile, *sslPrivateKeyFile)
			if err != nil {
				log.Error(err, "failed listen")
				os.Exit(1)
			}
			conf := &tls.Config{
				Certificates: []tls.Certificate{cer},
			}
			err = ssl_utils.AddSslToConfig(conf, *sslCipherSuite, *sslVersion)
			if err != nil {
				log.Error(err, "failed listen")
				os.Exit(1)
			}
			lst, err = tls.Listen(jrpc2.Network(*tcpAddress), *tcpAddress, conf)
		} else {
			log.Info("create listener with HTTP")
			lst, err = net.Listen(jrpc2.Network(*tcpAddress), *tcpAddress)
		}
		if err != nil {
			log.Error(err, "failed listen")
			os.Exit(1)
		}
		log.Info("listening", "on", lst.Addr())
		go loop(lst)
	}
	if runtime.GOOS == "linux" && len(*unixAddress) > 0 {
		if err := os.RemoveAll(*unixAddress); err != nil {
			log.Error(err, "failed to remove all address")
			os.Exit(1)
		}
		log.Info("create listener with unix sockets")
		lst, err := net.Listen(jrpc2.Network(*unixAddress), *unixAddress)
		if err != nil {
			log.Error(err, "failed listen")
			os.Exit(1)
		}
		log.Info("listening", "on", lst.Addr())
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
func createServicesMap(sharedService *ovsdb.Service, clientHandler *ovsdb.Handler) *jrpcHandler.Map {
	handlerMap := make(jrpcHandler.Map)
	handlerMap["list_dbs"] = jrpcHandler.New(sharedService.ListDbs)
	handlerMap["get_schema"] = jrpcHandler.New(sharedService.GetSchema)
	handlerMap["get_server_id"] = jrpcHandler.New(sharedService.GetServerId)
	handlerMap["convert"] = jrpcHandler.New(sharedService.Convert)

	handlerMap["transact"] = jrpcHandler.New(clientHandler.Transact)
	handlerMap["cancel"] = jrpcHandler.New(clientHandler.Cancel)
	handlerMap["monitor"] = jrpcHandler.New(clientHandler.Monitor)
	handlerMap["monitor_cancel"] = jrpcHandler.New(clientHandler.MonitorCancel)
	handlerMap["lock"] = jrpcHandler.New(clientHandler.Lock)
	handlerMap["steal"] = jrpcHandler.New(clientHandler.Steal)
	handlerMap["unlock"] = jrpcHandler.New(clientHandler.Unlock)
	handlerMap["monitor_cond"] = jrpcHandler.New(clientHandler.MonitorCond)
	handlerMap["monitor_cond_since"] = jrpcHandler.New(clientHandler.MonitorCondSince)
	handlerMap["monitor_cond_change"] = jrpcHandler.New(clientHandler.MonitorCondChange)
	handlerMap["set_db_change_aware"] = jrpcHandler.New(clientHandler.SetDbChangeAware)
	handlerMap["echo"] = jrpcHandler.New(clientHandler.Echo)
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
