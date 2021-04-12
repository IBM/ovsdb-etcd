.PHONY: install-tools
install-tools:
	./scripts/install_etcd.sh

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

.PHONY: server
server:
	$(MAKE) -C tests/e2e/ server &

.PHONY: tests
tests:
	go test -v ./...
