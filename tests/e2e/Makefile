ROOT_DIR := ../..

TCP_ADDRESS = 127.0.0.1:12345
UNIX_ADDRESS = /tmp/unix.soc

.PHONY: etcd
etcd:
	etcd

.PHONY: server
server: 
	cd $(ROOT_DIR) && go run pkg/cmd/server/server.go -tcp-address $(TCP_ADDRESS)  -unix-address $(UNIX_ADDRESS)

.PHONY: client
client:
	cd $(ROOT_DIR) && go run pkg/cmd/client/client.go -server $(TCP_ADDRESS)
