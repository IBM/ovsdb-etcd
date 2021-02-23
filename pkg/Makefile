CODE_GEN_DIR = ./json

#.PHONY: build
#build:
#	go build

.PHONY: generate
generate:
	go run cmd/codegenerator/generator.go -s ../json/ovn-nb.ovsschema  -d $(CODE_GEN_DIR)
	go run cmd/codegenerator/generator.go -s ../json/ovn-sb.ovsschema  -d $(CODE_GEN_DIR)
	go run cmd/codegenerator/generator.go -s ../json/_server.ovsschema  -d $(CODE_GEN_DIR)
	gofmt -s -w  $(CODE_GEN_DIR)

.PHONY: run_server
run_server: 
	go run cmd/server/server.go
