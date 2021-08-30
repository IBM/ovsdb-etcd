ROOT_DIR := ../..

TCP_ADDRESS = :12345
FULL_TCP_ADDRESS = 127.0.0.1:12345
UNIX_ADDRESS = /tmp/ovnnb_db.db

ETCD_NAME := ovsdb

DATABASE-PREFIX := ovsdb
SERVICE-NAME := nb
SCHEMA-FILE := ovn-nb.ovsschema
LOAD-SERVER-DATA :=true
PID-FILE :=/tmp/nb-ovsdb.pid
#SSL-PRIVKEY :=""
#SSL-CERT :=""
KEYS-ROOT-DIR-NAME := ./keys
SERVER-KEYS-DIR-NAME := $(KEYS-ROOT-DIR-NAME)/server
CLIENT-KEYS-DIR-NAME := $(KEYS-ROOT-DIR-NAME)/client

.PHONY: etcd
etcd:
	etcd \
		--name $(ETCD_NAME) \
		--data-dir /tmp/$(ETCD_NAME).etcd

SERVER_FILES := \
	$(ROOT_DIR)/pkg/cmd/server/server.go \
	$(ROOT_DIR)/pkg/cmd/server/testdata.go

SERVER_ARGS := \
		--tcp-address=$(TCP_ADDRESS) \
		--unix-address=$(UNIX_ADDRESS) \
		--schema-basedir=$(ROOT_DIR)/schemas \
		--database-prefix=$(DATABASE-PREFIX) \
		--service-name=$(SERVICE-NAME) \
		--schema-file=$(SCHEMA-FILE) \
		--load-server-data=$(LOAD-SERVER-DATA) \
		--pid-file=$(PID-FILE) \

CLIENT_ARGS := \
		--server=$(TCP_ADDRESS)

CLIENT_FILES := \
	$(ROOT_DIR)/pkg/cmd/client/client.go

define generate_private_public_keys_pair
	$(call generate_private_key,$(1))
	$(call generate_public_key,$(1))
endef

define generate_private_key
	openssl genrsa -out $(1).key 2048
endef

define generate_public_key
	openssl req -new -x509 -sha256 -key $(1).key -out $(1).pem -days 3650 -subj '/C=IL/ST=HAIFA/L=HAIFA/CN=www.ibm.com'
endef

.PHONY: remove-all-keys
remove-all-keys: 
	rm -rf $(KEYS-ROOT-DIR-NAME)

.PHONY: create-ssl-server-keys
create-ssl-server-keys: 
	mkdir -p $(SERVER-KEYS-DIR-NAME)
	$(call generate_private_public_keys_pair,$(SERVER-KEYS-DIR-NAME)/server)
.PHONY: create-ssl-client-keys

create-ssl-client-keys: 
	mkdir -p $(CLIENT-KEYS-DIR-NAME)
	$(call generate_private_public_keys_pair,$(CLIENT-KEYS-DIR-NAME)/client)

.PHONY: dbg-server
dbg-server: 
	dlv debug $(SERVER_FILES) -- $(SERVER_ARGS)

.PHONY: server
server: 
	go run $(SERVER_FILES) $(SERVER_ARGS)

.PHONY: ssl-server
ssl-server: 
	if [ ! -d "$(SERVER-KEYS-DIR-NAME)" ]; then $(MAKE) create-ssl-server-keys; fi
	$(eval SSL-PRIVKEY:=$(SERVER-KEYS-DIR-NAME)/server.key)
	$(eval SSL-CERT:=$(SERVER-KEYS-DIR-NAME)/server.pem)
	$(eval SERVER_ARGS := $(SERVER_ARGS) --ssl-privkey=$(SSL-PRIVKEY) --ssl-cert=$(SSL-CERT))
	go run $(SERVER_FILES) $(SERVER_ARGS)


.PHONY: dbg-client
dbg-client:
	dlv debug $(CLIENT_FILES) -- $(CLIENT_ARGS)

.PHONY: client
client:
	$(eval CLIENT_ARGS := $(CLIENT_ARGS) --use-ssl=false)
	go run $(CLIENT_FILES) $(CLIENT_ARGS)

.PHONY: ssl-client
ssl-client: 
	$(eval CLIENT_ARGS := $(CLIENT_ARGS) --use-ssl=true)
	go run $(CLIENT_FILES) $(CLIENT_ARGS)

.PHONY: ssl-ovsdb-client
ssl-ovsdb-client: 
	if [ ! -d "$(CLIENT-KEYS-DIR-NAME)" ]; then $(MAKE) create-ssl-client-keys; fi
	$(eval SSL-PRIVKEY:=$(CLIENT-KEYS-DIR-NAME)/client.key)
	$(eval SSL-CERT:=$(CLIENT-KEYS-DIR-NAME)/client.pem)
	ovsdb-client list-dbs ssl:$(FULL_TCP_ADDRESS) --ca-cert=none --private-key=$(SSL-PRIVKEY) --certificate=$(SSL-CERT)

INSTALL_TOOLS += /usr/bin/pv
/usr/bin/pv:
	sudo apt-get install -y pv

INSTALL_TOOLS += /usr/bin/ovsdb-client
/usr/bin/ovsdb-client:
	sudo apt-get install -y ovn-common

INSTALL_TOOLS += /usr/local/sbin/demo-magic.sh
/usr/local/sbin/demo-magic.sh:
	sudo wget -O $@ https://raw.githubusercontent.com/paxtonhare/demo-magic/master/demo-magic.sh

.PHONY: demo
demo: $(INSTALL_TOOLS)
	./demo.sh
