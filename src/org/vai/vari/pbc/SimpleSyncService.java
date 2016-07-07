package org.vai.vari.pbc;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.*;
import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import org.kohsuke.args4j.*;
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
	public void setSourceKeyStore(File f) {
		sourceKeyStore = Optional.of(f);
	}
	private Optional<File> sourceKeyStore = Optional.empty();
	
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
	
	private Client sourceClient;
	private Client targetClient;
	
    /**
     * Main method
     * @param args
     * @throws IOException 
     * @throws GeneralSecurityException 
     */
    public static void main(String[] args) throws GeneralSecurityException, IOException {
        
    	final SimpleSyncService service = new SimpleSyncService();
        final CmdLineParser parser = new CmdLineParser(service);
        parser.getProperties().withShowDefaults(false);
        try {
        	parser.parseArgument(args);
        }
        catch (CmdLineException e) {
        	System.err.println(e.getMessage());
        	System.err.println("usage: SimpleSyncService [options]");
        	parser.printUsage(System.err);
        	System.exit(1);
        }
        
        if (service.help) {
    		System.err.println("usage: SimpleSyncService [options]");
        	parser.printUsage(System.out);
    		return;
    	}
        
        service.init();
    	service.run();
    }
    
    public void init() throws GeneralSecurityException, IOException {

    	System.setProperty("https.protocols", "TLSv1.2");
        ClientBuilder builder = ClientBuilder.newBuilder();
        
        // Use peer certificate authentication
        if (sourceKeyStore.isPresent()) {
	        KeyStore keystore = KeyStore.getInstance("PKCS12");
	        keystore.load(new FileInputStream(sourceKeyStore.get()), sourcePassword == null ? null : sourcePassword.toCharArray());
        	builder.keyStore(keystore, sourcePassword == null ? "" : sourcePassword);
        }
        sourceClient = builder.build();
        
        if (!targetUri.isPresent())
        	return;
        
        targetClient = ClientBuilder.newBuilder()
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
    }
    
    /**
     * run method
     * @throws GeneralSecurityException 
     * @throws IOException 
     */
    public int run() throws GeneralSecurityException, IOException {
    	
    	// GET
    	Response response = sourceClient.target(sourceUri).request().get();
    	int httpCode = response.getStatus();
        if (httpCode < 200 || httpCode >= 300) {
        	throw new IOException("unexpected response from '" + sourceUri + "': " + httpCode);
        }
        String sourceData = response.readEntity(String.class);

    	// pretty print JSON to console if no target specified
    	if (targetClient == null) {
    		ObjectMapper mapper = new ObjectMapper();
    		Object json = mapper.readValue(sourceData, Object.class);
        	System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
    		return 0;
    	}
        
    	// send
    	JsonNode jsonRoot = new ObjectMapper().readTree(sourceData);
    	if (jsonRoot.size() == 1 && jsonRoot.elements().next().isArray()) {
    		// send multiple documents
    		for (final JsonNode objNode : jsonRoot.elements().next()) {
    	        httpCode = send(objNode, targetClient);
    	    }
    		return httpCode;
    	} else {
    		// send one document
    		return send(jsonRoot, targetClient);
    	}
    }
    
    private int send(JsonNode data, Client client) throws IOException {
		String id = idField.isPresent() ? data.get(idField.get()).asText() : "";

        // POST
        Response response = client.target(targetUri.get() + id)
    			.request(MediaType.APPLICATION_JSON_TYPE)
    			.post(Entity.entity(new ObjectMapper().writeValueAsString(data), MediaType.APPLICATION_JSON));
    	return response.getStatus();
    }
}

