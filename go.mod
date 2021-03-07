module github.com/ibm/ovsdb-etcd

go 1.15

require (
	github.com/cenk/hub v1.0.1 // indirect
	github.com/creachadair/jrpc2 v0.12.0
	github.com/ebay/libovsdb v0.0.0-20190718202342-e49b8c4e1142
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/google/uuid v1.2.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.4.0
	go.etcd.io/etcd/client/v3 v3.0.0-20210127081512-a4fac14353e7
	golang.org/x/net v0.0.0-20201202161906-c7110b5ffcbb // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20210112080510-489259a85091 // indirect
	golang.org/x/tools v0.0.0-20201224043029-2b0845dc783e // indirect
	google.golang.org/genproto v0.0.0-20201210142538-e3217bee35cc // indirect
	k8s.io/klog v1.0.0
	k8s.io/klog/v2 v2.6.0
)

replace (
	//	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.3
	github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
	github.com/creachadair/jrpc2 v0.12.0 => github.com/ibm/jrpc2 v1.12.0
	go.etcd.io/etcd => go.etcd.io/etcd v0.0.0-20200520232829-54ba9589114f
	go.etcd.io/etcd/api/v3 => go.etcd.io/etcd/api/v3 v3.0.0-20201103155942-6e800b9b0161
	go.etcd.io/etcd/pkg/v3 => go.etcd.io/etcd/pkg/v3 v3.0.0-20201103155942-6e800b9b0161
	google.golang.org/grpc v1.29.1 => google.golang.org/grpc v1.26.0
	google.golang.org/grpc v1.30.0 => google.golang.org/grpc v1.26.0
)
