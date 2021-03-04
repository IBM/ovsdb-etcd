.PHONY: install-tools
install-tools:
	@echo "nothing for now"

VERIFY += generate
.PHONY: generate
generate: CODE_GEN_DIR = pkg/json
generate: GEN = pkg/cmd/codegenerator/generator.go
generate:
	go run $(GEN) -s ./json/ovn-nb.ovsschema -d $(CODE_GEN_DIR)
	go run $(GEN) -s ./json/ovn-sb.ovsschema -d $(CODE_GEN_DIR)
	go run $(GEN) -s ./json/_server.ovsschema -d $(CODE_GEN_DIR)

VERIFY += fmt
.PHONY: fmt
fmt:
	go fmt ./... || true

VERIFY += vet
.PHONY: vet
vet:
	go vet ./... || true

VERIFY += fix
.PHONY: fix
fix:
	go fix ./... || true

VERIFY += tidy
.PHONY: tidy
tidy:
	go mod tidy || true

.PHONY: verify
verify: $(VERIFY)
	git diff --exit-code || true

.PHONY: tests
tests:
	go test ./...

.PHONY: server
server: TCP_ADDRESS = 127.0.0.1:12345
server: UNIX_Address = /tmp/unix.soc 
server: 
	go run pkg/cmd/server/server.go -tcp-address $(TCP_ADDRESS)  -unix-address $(UNIX_Address)
