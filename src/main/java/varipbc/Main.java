package varipbc;

import javax.ws.rs.client.*;
import javax.ws.rs.core.Response;
import java.security.KeyStore;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;

/**
 * Main class.
 *
 */
public class Main {

    /**
     * Main method.
     * @param args
     * @throws IOException
     * @throws NoSuchAlgorithmException 
     * @throws KeyStoreException 
     * @throws CertificateException 
     */
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateException {
        String target = args[0];
    	
    	System.setProperty("https.protocols", "TLSv1.2");
        ClientBuilder builder = ClientBuilder.newBuilder();
        
        if (args.length > 1) {
	        String[] cert = args[1].split(":");
	        KeyStore keystore = KeyStore.getInstance("PKCS12");
	        keystore.load(new FileInputStream(new File(cert[0])), cert[1].toCharArray());
        	builder = builder.keyStore(keystore, cert[1]);
        }
        
        Client client = builder.build();
    	Response response = client.target(target).request().get();
    	System.out.println(response.getStatus());
    	System.out.println(response.readEntity(String.class));
    }
}

