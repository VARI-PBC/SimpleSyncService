# SimpleSyncService

Console app that transfers data between two REST endpoints secured by TLSv1.2. Currently uses a peer (client) certificate for the source and HTTP basic auth for the target. Uses the JAX-RS ([Jersey](https://jersey.java.net/)) [Client API](https://jersey.java.net/documentation/latest/client.html).

```
usage: SimpleSyncService [options]
 --help (-h)              : print this message
 --srcKeystore <file>     : PKCS #12 file for source peer (client) certificate
 --srcPassword <password> : password for source peer (client) certificate
 --srcUri <uri>           : URI of the data source endpoint (required)
 --tgtPassword <password> : password for target auth
 --tgtUri <uri>           : URI of the target endpoint
 --tgtUsername <username> : username for target auth
```

Example command line:

    java org.vai.vari.pbc.SimpleSyncService --srcUri https://source.org/route/ --srcKeystore /path/to/client-cert.p12 --srcPassword password --tgtUri https://target.org/route/ --tgtUsername username --tgtPassword password
