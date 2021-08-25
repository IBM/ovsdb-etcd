module github.com/ibm/ovsdb-etcd

go 1.15

require (
	github.com/cenk/hub v1.0.1 // indirect
	github.com/cenkalti/hub v1.0.1 // indirect
	github.com/cenkalti/rpc2 v0.0.0-20210220005819-4a29bc83afe1
	github.com/creachadair/jrpc2 v0.12.0
	github.com/go-logr/logr v0.4.0
	github.com/google/uuid v1.2.0
	github.com/jinzhu/copier v0.3.0
	github.com/lithammer/shortuuid/v3 v3.0.7
	github.com/onsi/ginkgo v1.16.1
	github.com/onsi/gomega v1.11.0
	github.com/pkg/errors v0.8.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.1-0.20210116013205-6990a05d54c2
	go.etcd.io/etcd/api/v3 v3.5.0-alpha.0
	go.etcd.io/etcd/client/v3 v3.5.0-alpha.0
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	google.golang.org/genproto v0.0.0-20201210142538-e3217bee35cc // indirect
	k8s.io/klog v1.0.0
	k8s.io/klog/v2 v2.6.0
)

replace (
	github.com/creachadair/jrpc2 v0.12.0 => github.com/ibm/jrpc2 v1.12.5
	google.golang.org/grpc v1.29.1 => google.golang.org/grpc v1.26.0
	google.golang.org/grpc v1.30.0 => google.golang.org/grpc v1.26.0
)
