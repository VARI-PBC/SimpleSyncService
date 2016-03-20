# SimpleSyncService

Thus far, this is a very raw example of console app that connects to a TLSv1.2 endpoint, (optionally) using a peer (client) certificate.

Command line arguments are:

1. the endpoint target
2. the full path to the client .p12 (PKCS#12) certificate store and its password, separated by a colon


Example command line:

    java varipbc.Main https://host.domain.com/path/ /path/to/client-cert.p12:password
