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

## etcd schema used in the project
In the project we use the following etcd key formation convention:
* all key parts are slash ("/") separated
* each key should contain 5 slash ("/") separated parts, and each part should not be empty.  
* keys start from deployment defined prefix, e.g. ovsdb
* the next key part defines the deployment service, e.g. "sbdb" or "nbdb"
* after that, we have a database server name, e.g. `OVN_Northbound`, `OVN_Southbound`, `_Server`. There  is an 
  additional internal entry `_` for locks and comments.
* next key element defines table names according to the relevant schema. Locks and comments are stored in the internal 
  database under the `_locks` and `_comments` entries.
* the last key element is the `uuid` of a table row, `lock ID`, or a comment's `timestamp`.
* in order to guarantee that `_Server/Database` will contains only entries per running databases, its last key element
  is not uuid, but the database server name.
  
The above explanation can be demonstrated as:
- data:           <prefix>/<service>/<dbName>/<table>/<uuid> --> <row>
- locks:          <prefix>/<service>/_/_locks/<lockid> --> nil
- comments:       <prefix>/<service>/_/_comments/<timestamp> --> <comment>

