# Differences between Json RPC implementation by OVSDB and Golang standard library.

## RPC registration
Golang RPC package provides access to the exported methods of an object across a network or other I/O connection.  
A server registers an object, making it visible as a service with the name of the type of the object. But the published 
method has a name of Type.Method, where Type is golang type, and it cannot be nil, and the published method should be
publicly available, which means it should start from uppercase character.
All OVSDB methods are lowercase strings without any type.

The fix is in `ServerCodec.ReadRequestHeader`

## Request parameters parsing
Golang JSON RPC doesn't support array of parameters, it returns only the first parameter. 

The fix is in `ServerCodec.ReadRequestBody`

## Notifications
Json RPC defines Notifications as Requests without responses, so they contain request Ids = null. However, they are sent 
by the client to the server. OVSDB defines opposite direction, Notifications from the Server to the Client.

A fix is required.

## None standard Json encoding
We are working on proprietary encoder and decoder.  


 