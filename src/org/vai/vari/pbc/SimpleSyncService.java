package org.vai.vari.pbc;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.*;
import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import java.security.KeyStore;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.Optional;
import org.kohsuke.args4j.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Main class
 */
public class SimpleSyncService {

	@Option(name = "--help", aliases = "-h", usage = "print this message", help = true)
	private boolean help;
	 
	@Option(name = "--srcUri", metaVar = "<uri>", required = true,
	        usage = "URI of the data source endpoint (required)")
	public URI sourceUri;

	@Option(name = "--srcKeystore", metaVar = "<file>",
	        usage = "PKCS #12 file for source peer (client) certificate", depends = {"--srcUri"})
	public void setSrcKeystore(File f) {
		sourceKeystore = Optional.of(f);
	}
	private Optional<File> sourceKeystore = Optional.empty();
	
	@Option(name = "--srcPassword", metaVar = "<password>",
	        usage = "password for source peer (client) certificate", depends = {"--srcUri"})
	public String sourcePassword = "";

	@Option(name = "--tgtUri", metaVar = "<uri>",
	        usage = "URI of the target endpoint")
	public void setTargetUri(URI uri) {
		targetUri = Optional.of(uri);
	}
	private Optional<URI> targetUri = Optional.empty();
	
	@Option(name = "--tgtUsername", metaVar = "<username>",
	        usage = "username for target auth", depends = {"--tgtUri"})
	public void setTargetUsername(String u) {
		targetUsername = Optional.of(u);
	}
	private Optional<String> targetUsername = Optional.empty();
	
	@Option(name = "--tgtPassword", metaVar = "<password>",
	        usage = "password for target auth", depends = {"--tgtUri"})
	public String targetPassword;
	
	@Option(name = "--idField", metaVar = "<id field>",
	        usage = "field name to use for unique ID of document", depends = {"--tgtUri"})
	public void setIdField(String f) {
		idField = Optional.of(f);
	}
	private Optional<String> idField = Optional.empty();
	
    /**
     * Main method
     * @param args
     */
    public static void main(String[] args) {
        
    	final SimpleSyncService service = new SimpleSyncService();
        final CmdLineParser parser = new CmdLineParser(service);
        parser.getProperties().withShowDefaults(false);
        try {
        	parser.parseArgument(args);
        	if (service.help) {
        		System.err.println("usage: SimpleSyncService [options]");
            	parser.printUsage(System.out);
        		return;
        	}
        	service.run();
        }
        catch (CmdLineException e) {
        	System.err.println(e.getMessage());
        	System.err.println("usage: SimpleSyncService [options]");
        	parser.printUsage(System.err);
        }
        catch (Exception e) {
        	e.printStackTrace();
        }
    }
    
    /**
     * run method
     */
    public void run() throws KeyStoreException, NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException {
    	
    	System.setProperty("https.protocols", "TLSv1.2");
        ClientBuilder builder = ClientBuilder.newBuilder();
        
        // Use peer certificate authentication
        if (sourceKeystore.isPresent()) {
	        KeyStore keystore = KeyStore.getInstance("PKCS12");
	        keystore.load(new FileInputStream(sourceKeystore.get()), sourcePassword == null ? null : sourcePassword.toCharArray());
        	builder = builder.keyStore(keystore, sourcePassword == null ? "" : sourcePassword);
        }
        
        // GET
    	Response getResponse = builder.build().target(sourceUri).request().get();
    	String sourceData = getResponse.readEntity(String.class);
    	
    	// pretty print JSON to console if no target specified
    	if (!targetUri.isPresent()) {
    		ObjectMapper mapper = new ObjectMapper();
    		Object json = mapper.readValue(sourceData, Object.class);
        	System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
    		return;
    	}
    	
    	Client targetClient = ClientBuilder.newBuilder()
    			// Set up a HostnameVerifier to work around the lack of SNI support on the production server. 
    			.hostnameVerifier(new HostnameVerifier()
        {
    		public boolean verify(String hostname, SSLSession session)
            {
                if (hostname.equals(targetUri.get().getHost()))
                    return true;
                return false;
            }
        }).build();

        // Use HTTP basic authentication
        if (targetUsername.isPresent()) {
        	HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(targetUsername.get(), targetPassword);
        	targetClient.register(feature);
        }
        
    	JsonNode json = new ObjectMapper().readTree(sourceData);
    	if (json.size() == 1 && json.elements().next().isArray()) {
    		for (final JsonNode objNode : json.elements().next()) {
    	        send(objNode, targetClient);
    	    }
    	}
    }
    
    private void send(JsonNode data, Client client) throws JsonProcessingException {
		String id = idField.isPresent() ? data.get(idField.get()).asText() : "";

        // POST
        Response targetResponse = client.target(targetUri.get() + id)
    			.request(MediaType.APPLICATION_JSON_TYPE)
    			.post(Entity.entity(new ObjectMapper().writeValueAsString(data), MediaType.APPLICATION_JSON));
    	System.out.println(targetResponse.getStatus());
    	System.out.println(targetResponse.readEntity(String.class));    
    }
}

