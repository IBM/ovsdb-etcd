ROOT_DIR := .
SERVER_EXECUTEBLE := "$(ROOT_DIR)/pkg/cmd/server/server"

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

.PHONY: docker-build
docker-build: build
	@echo "checking for OVN_KUBERNETES_ROOT" && [ -n "${OVN_KUBERNETES_ROOT}" ]
	cp ${OVN_KUBERNETES_ROOT}/dist/images/ovndb-raft-functions.sh dist/images/.
	cp ${OVN_KUBERNETES_ROOT}/dist/images/ovnkube.sh dist/images/.
	docker build . -t etcd -f dist/images/Dockerfile.etcd
	docker build . -t ovsdb-etcd -f dist/images/Dockerfile.ovsdb-etcd

.PHONY: docker-push
docker-push:
	@echo "checking for OVSDB_ETCD_REPOSITORY" && [ -n "${OVSDB_ETCD_REPOSITORY}" ]
	docker tag ovsdb-etcd ${OVSDB_ETCD_REPOSITORY}/ovsdb-etcd
	docker push ${OVSDB_ETCD_REPOSITORY}/ovsdb-etcd
	docker tag etcd ${OVSDB_ETCD_REPOSITORY}/etcd
	docker push ${OVSDB_ETCD_REPOSITORY}/etcd

.PHONY: docker
docker: docker-build docker-push

export KUBECONFIG=${HOME}/admin.conf

OVNDB_ETCD_TCPDUMP ?= 'true'
.PHONY: ovnk-deploy
ovnk-deploy:
	@echo "checking for OVN_KUBERNETES_ROOT" && [ -n "${OVN_KUBERNETES_ROOT}" ]
	@echo "checking for OVSDB_ETCD_REPOSITORY" && [ -n "${OVSDB_ETCD_REPOSITORY}" ]
	cd ${OVN_KUBERNETES_ROOT}/contrib && ./kind.sh \
		--ovn-etcd-image "${OVSDB_ETCD_REPOSITORY}/etcd:latest" \
		--ovn-ovsdb-etcd-image "${OVSDB_ETCD_REPOSITORY}/ovsdb-etcd:latest" \
		--master-loglevel 7 \
		--node-loglevel 7 \
		--dbchecker-loglevel 7 \
		--ovndb-etcd-tcpdump ${OVNDB_ETCD_TCPDUMP} \
		--ovn-loglevel-northd '-vconsole:dbg -vfile:dbg' \
		--ovn-loglevel-nbctld '-vconsole:dbg -vfile:dbg' \
		--ovn-loglevel-controller '-vconsole:dbg -vfile:dbg'

.PHONY: ovnk-status
ovnk-status:
	kubectl -n=ovn-kubernetes get pods

.PHONY: ovnk-delete
ovnk-delete:
	@echo "checking for OVN_KUBERNETES_ROOT" && [ -n "${OVN_KUBERNETES_ROOT}" ]
	cd ${OVN_KUBERNETES_ROOT}/contrib && ./kind.sh --delete
