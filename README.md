[![Build Status](https://travis-ci.com/IBM/ovsdb-etcd.svg?branch=master)](https://travis-ci.com/IBM/ovsdb-etcd "Travis")
[![Go Report Card](https://goreportcard.com/badge/github.com/IBM/ovsdb-etcd)](https://goreportcard.com/report/github.com/IBM/ovsdb-etcd)

# OVSDB ETCD Backend (PoC)

A PoC project to store OVN database data into an etcd cluster, as by doing so
we can leverage the advantages of the widely used distributes key-value store -
etcd. As an alternative to the propriety transactional store currently serving
OVSDB. The goal is that this be a drop in replacement which is fully compatible
with any of the existing ovsdb clients.

You will find the spec here
[rfc7047](https://tools.ietf.org/html/rfc7047#page-19),
and you will find a description of the extensions beyond the spec here
[ovsdb-server](https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/).

Our initial goal is to demonstrate the work on a k8s cluster, but later plan to
support the generic use case.
