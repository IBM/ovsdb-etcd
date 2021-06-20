ROOT_DIR := .
SERVER_EXECUTEBLE := "$(ROOT_DIR)/pkg/cmd/server/server"

SERVER_FILES := \
	$(ROOT_DIR)/pkg/cmd/server/server.go \
	$(ROOT_DIR)/pkg/cmd/server/testdata.go

CACHE := /tmp/.cache
ETCD_VERSION ?= v3.4.16
ETCD_PACKAGE := etcd-${ETCD_VERSION}-linux-amd64
ETCD_TAR := ${ETCD_PACKAGE}.tar.gz
ETCD_URL := https://github.com/coreos/etcd/releases/download/${ETCD_VERSION}/${ETCD_TAR}
ETCDCTL := ${ETCD_PACKAGE}/etcdctl
ETCD := ${ETCD_PACKAGE}/etcd

${CACHE}:
	mkdir -p ${CACHE}

${CACHE}/${ETCD_TAR}: ${CACHE}
	cd ${CACHE} && curl -L ${ETCD_URL} -o ${ETCD_TAR}

${CACHE}/${ETCD_PACKAGE}: ${CACHE}/${ETCD_TAR}
	cd ${CACHE} && tar xvfz ${ETCD_TAR}
	cd ${CACHE} && touch ${ETCD_PACKAGE}

${CACHE}/${ETCDCTL} ${CACHE}/${ETCD}: ${CACHE}/${ETCD_PACKAGE}

.PHONY: install-tools
install-tools: install-etcd
	mkdir /tmp/openvswitch/

.PHONY: install-etcd
install-etcd: /usr/local/bin/etcd /usr/local/bin/etcdctl

/usr/local/bin/etcd: ${CACHE}/${ETCD}
	sudo cp $< $@

/usr/local/bin/etcdctl: ${CACHE}/${ETCDCTL}
	sudo cp $< $@

docker-downloads: dist/images/etcd dist/images/etcdctl


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

dist/images/etcd: ${CACHE}/${ETCD}
	cp $< $@

dist/images/etcdctl: ${CACHE}/${ETCDCTL}
	cp $< $@

docker-downloads: dist/images/etcd dist/images/etcdctl

check-env:
	@if [ -z "${OVN_KUBERNETES_ROOT}" ]; then \
		echo "missing env OVN_KUBERNETES_ROOT"; \
		exit 1; \
	fi
	@if [ -z "${OVSDB_ETCD_REPOSITORY}" ]; then \
		echo "missing env OVSDB_ETCD_REPOSITORY"; \
		exit 1; \
	fi


.PHONY: docker-build
docker-build: check-env build docker-downloads
	cp ${OVN_KUBERNETES_ROOT}/dist/images/ovndb-raft-functions.sh dist/images/.
	cp ${OVN_KUBERNETES_ROOT}/dist/images/ovnkube.sh dist/images/.
	docker build . -t etcd -f dist/images/Dockerfile.etcd
	docker tag etcd ${OVSDB_ETCD_REPOSITORY}/etcd
	docker build . -t ovsdb-etcd -f dist/images/Dockerfile.ovsdb-etcd
	docker tag ovsdb-etcd ${OVSDB_ETCD_REPOSITORY}/ovsdb-etcd

.PHONY: docker-push
docker-push: check-env
	docker push ${OVSDB_ETCD_REPOSITORY}/etcd
	docker push ${OVSDB_ETCD_REPOSITORY}/ovsdb-etcd

.PHONY: docker
docker: docker-build docker-push

export KUBECONFIG=${HOME}/admin.conf

OVNDB_ETCD_TCPDUMP ?= 'true'
.PHONY: ovnk-deploy
ovnk-deploy: check-env
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
ovnk-delete: check-env
	cd ${OVN_KUBERNETES_ROOT}/contrib && ./kind.sh --delete
