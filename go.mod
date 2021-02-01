module github.com/roytman/ovsdb-etcd

go 1.15

require (
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200910180754-dd1b699fc489
	go.etcd.io/etcd/client/v3 v3.0.0-20210127081512-a4fac14353e7
	google.golang.org/genproto v0.0.0-20201210142538-e3217bee35cc
)

replace (
	go.etcd.io/etcd/api/v3 => go.etcd.io/etcd/api/v3 v3.0.0-20201103155942-6e800b9b0161
	go.etcd.io/etcd/pkg/v3 => go.etcd.io/etcd/pkg/v3 v3.0.0-20201103155942-6e800b9b0161
	google.golang.org/grpc v1.30.0 => google.golang.org/grpc v1.26.0
)
