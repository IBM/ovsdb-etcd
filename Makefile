ROOT_DIR := .
SERVER_EXECUTEBLE=$(ROOT_DIR)/pkg/cmd/server/server

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
build:
	CGO_ENABLED=0 go build -o $(SERVER_EXECUTEBLE) $(SERVER_FILES)

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
	docker build . -t etcd -f dist/images/etcd/Dockerfile

.PHONY: image-server
image-server: build
	docker build . -t server -f dist/images/server/Dockerfile
