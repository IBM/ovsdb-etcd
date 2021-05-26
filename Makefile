ROOT_DIR := .
SERVER_EXECUTEBLE := "$(ROOT_DIR)/pkg/cmd/server/server"

OVN_KUBERNETES_ROOT ?= $(ROOT_DIR)/../../ovn-org/ovn-kubernetes

SERVER_FILES := \
	$(ROOT_DIR)/pkg/cmd/server/server.go \
	$(ROOT_DIR)/pkg/cmd/server/testdata.go

.PHONY: install-tools
install-tools:
	./scripts/install_etcd.sh
	mkdir /tmp/openvswitch/

VERIFY += generate
.PHONY: generate
generate: CODE_GEN_DIR = pkg/types
generate: GEN = pkg/cmd/generator/generator.go
generate:
	go run $(GEN) -s ./schemas/ovn-nb.ovsschema -d $(CODE_GEN_DIR)
	go run $(GEN) -s ./schemas/ovn-sb.ovsschema -d $(CODE_GEN_DIR)
	go run $(GEN) -s ./schemas/_server.ovsschema -d $(CODE_GEN_DIR)
	gofmt -s -w $(CODE_GEN_DIR)

VERIFY += fmt
.PHONY: fmt
fmt:
	go fmt ./...

VERIFY += vet
.PHONY: vet
vet:
	go vet ./...

VERIFY += fix
.PHONY: fix
fix:
	go fix ./...

VERIFY += tidy
.PHONY: tidy
tidy:
	go mod tidy

.PHONY: verify
verify: $(VERIFY)
	git diff --exit-code

.PHONY: etcd
etcd:
	$(MAKE) -C tests/e2e/ etcd &

.PHONY: build
build: GIT_COMMIT := "$(shell git rev-list -1 HEAD)"
build:
	CGO_ENABLED=0 go build -ldflags "-X main.GitCommit=$(GIT_COMMIT)" -o $(SERVER_EXECUTEBLE) $(SERVER_FILES)

.PHONY: server
server:
	$(MAKE) -C tests/e2e/ server &

.PHONY: north-server
north-server:
	$(MAKE) -C tests/e2e/ server -e TCP_ADDRESS=:6641 UNIX_ADDRESS=/tmp/ovnnb_db.db DATABASE-PREFIX=ovsdb SERVICE-NAME=nb SCHEMA-FILE=ovn-nb.ovsschema LOAD-SERVER-DATA=FALSE PID-FILE=/tmp/nb-ovsdb.pid &

.PHONY: south-server
south-server:
	$(MAKE) -C tests/e2e/ server -e TCP_ADDRESS=:6642 UNIX_ADDRESS=/tmp/ovnsb_db.db DATABASE-PREFIX=ovsdb SERVICE-NAME=sb SCHEMA-FILE=ovn-sb.ovsschema LOAD-SERVER-DATA=FALSE PID-FILE=/tmp/sb-ovsdb.pid &

.PHONY: tests
tests:
	go test -v ./...

.PHONY: image-etcd
image-etcd:
	docker build . -t etcd -f dist/images/Dockerfile.etcd

.PHONY: image-server
image-server: build
	docker build . -t server -f dist/images/Dockerfile.ovsdb-etcd

.PHONY: ovn-kubernetes-build
ovn-kubernetes-build:
	$(MAKE) build
	$(MAKE) image-server
	$(MAKE) image-etcd
	./scripts/pushDocker
	cp dist/ovn-kubernetes/dist/images/ovndb-raft-functions.sh ${OVN_KUBERNETES_ROOT}/dist/images/
	cp dist/ovn-kubernetes/dist/images/ovnkube.sh ${OVN_KUBERNETES_ROOT}/dist/images/
	awk '{sub("XXREPO","${OVSDB_ETCD_REPOSITORY}")}1' dist/ovn-kubernetes/dist/templates/ovnkube-db.yaml.j2  > ${OVN_KUBERNETES_ROOT}/dist/templates/ovnkube-db.yaml.j2
	cp  dist/ovn-kubernetes/dist/templates/ovnkube-node.yaml.j2  ${OVN_KUBERNETES_ROOT}/dist/templates/ovnkube-node.yaml.j2
