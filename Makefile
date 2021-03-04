CODE_GEN_DIR = pkg/json
GEN = pkg/cmd/codegenerator/generator.go

TCP_ADDRESS = 127.0.0.1:12345
UNIX_Address = /tmp/unix.soc 


#.PHONY: build
#build:
#	go build

.PHONY: generate
generate:
	go run $(GEN) -s ./json/ovn-nb.ovsschema  -d $(CODE_GEN_DIR)
	go run $(GEN) -s ./json/ovn-sb.ovsschema  -d $(CODE_GEN_DIR)
	go run $(GEN) -s ./json/_server.ovsschema  -d $(CODE_GEN_DIR)
	gofmt -s -w  $(CODE_GEN_DIR)

.PHONY: run_server
run_server: 
	go run pkg/cmd/server/server.go -tcp-address $(TCP_ADDRESS)  -unix-address $(UNIX_Address)
