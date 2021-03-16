ROOT_DIR := ../..

TCP_ADDRESS = 127.0.0.1:12345
UNIX_ADDRESS = /tmp/unix.soc

ETCD_NAME := ovsdb

.PHONY: etcd
etcd:
	etcd \
		--name $(ETCD_NAME) \
		--data-dir /tmp/$(ETCD_NAME).etcd

SERVER_ARGS := \
		-tcp-address $(TCP_ADDRESS) \
		-unix-address $(UNIX_ADDRESS) \
		-schema-basedir $(ROOT_DIR)/schemas
.PHONY: dbg-server
dbg-server: 
	dlv debug $(ROOT_DIR)/pkg/cmd/server/server.go -- $(SERVER_ARGS)

.PHONY: server
server: 
	go run $(ROOT_DIR)/pkg/cmd/server/server.go $(SERVER_ARGS)

CLIENT_ARGS := \
		-server $(TCP_ADDRESS)

.PHONY: dbg-client
dbg-client:
	dlv debug $(ROOT_DIR)/pkg/cmd/client/client.go -- $(CLIENT_ARGS)

.PHONY: client
client:
	go run $(ROOT_DIR)/pkg/cmd/client/client.go $(CLIENT_ARGS)
