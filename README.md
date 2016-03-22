# SimpleSyncService

Thus far, this is a very raw example of console app that connects to a TLSv1.2 endpoint, (optionally) using a peer (client) certificate.
Example was take from:

* https://jersey.java.net/documentation/latest/client.html#d0e5140
* https://jersey.java.net/apidocs-javax.jax-rs/2.0.1/javax/ws/rs/client/ClientBuilder.html#keyStore-java.security.KeyStore-char:A-

Command line arguments are:

1. the endpoint target
2. the full path to the client .pfx/.p12 (PKCS#12) certificate store
3. the password for the .pfx/.p12 certificate store


Example command line:

    java varipbc.Main https://host.domain.com/path/ /path/to/client-cert.p12 password
