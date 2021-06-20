[![Build Status](https://travis-ci.com/IBM/ovsdb-etcd.svg?branch=master)](https://travis-ci.com/IBM/ovsdb-etcd "Travis")
[![Go Report Card](https://goreportcard.com/badge/github.com/IBM/ovsdb-etcd)](https://goreportcard.com/report/github.com/IBM/ovsdb-etcd)

# OVSDB ETCD (PoC)

ovsdb-etcd is an alternative ovsdb-server implementation, for use in
conjunction with ovn-kubernetes, written in golang and using etcd as a backend. 
It is a fully compatible drop-in replacement for ovsdb-server and adheres to
spec [rfc7047](https://tools.ietf.org/html/rfc7047#page-19) and spec extension
[ovsdb-server](https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/).

## Implementation of ovsdb-etcd

The objects are mapped to etcd as follows:

```
data:           <prefix>/<service>/<dbName>/<table>/<uuid> --> <row>
locks:          <prefix>/<service>/_/_locks/<lockid> --> nil
comments:       <prefix>/<service>/_/_comments/<timestamp> --> <comment>
```

As ovsdb transactions have different semantics than etcd transactions, the
initial implementation consists of:
- receiving the transaction-request in ovsdb semantics
- catching the global lock
- etcd get the pre-transaction **tables**
- process ovsdb semantics on memory
- etcd post the post-transaction **rows**
- releasing the global lock
- sending the transaction-response in ovsdb semantics

An ovsdb monitor enrollment is implemented as:
- receiving monitor-request in ovsdb semantics
- enrolling to get etcd events per entire **database** (to support cross **tables** consistency)
- sending monitor-response in ovsdb semantics

An ovsdb monitor event is implemented as:
- receiving monitor-event in etcd semantics
- extract from etcd **database** event all ovsdb **table** events (package as one update to support cross **tables** consistency)
- sending monitor-event is ovsdb semantics

## To start developing ovsdb-etcd

You must have a working [Go Environment](https://golang.org/doc/install).

Then clone ovn-kubernetes from our fork:

```bash
git clone https://github.com/ibm/ovn-kubernetes
pushd ovn-kubernetes
git checkout ovsdb-etcd2
export OVN_KUBERNETES_ROOT=$PWD
popd
```

Then clone ovsdb-etcd and build and push docker images:

```bash
git clone https://github.com/IBM/ovsdb-etcd
make docker-build
```

Setup your docker.io credentials:

```
export CR_PASSWORD=<DOCKER_PASSWORD>
export CR_USERNAME=<DOCKER_USENAME>
export OVSDB_ETCD_REPOSITORY=$CR_USERNAME
```
Login to docker and push (optional):

```
echo $CR_PASSWORD | docker login $CR_REPO -u $CR_USERNAME --password-stdin
make docker-push
```

Disable tcpdump logging (optional):

```
export OVNDB_ETCD_TCPDUMP='false'
```

Deploy onto a kind cluster:


```bash
make ovnk-deploy
make ovnk-status
```

## Support for ovsdb-etcd

- open an [issue](https://github.com/IBM/ovsdb-etcd/issues).
- post on [slack](https://coreos.slack.com/archives/C0258ALGDA6) for RedHat users.
