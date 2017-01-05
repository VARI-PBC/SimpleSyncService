package org.vai.vari.pbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class SimpleSyncServiceManager {
	
	public String sourceUri;
	public String sourceKeyStore;
	public String sourcePassword;
	public String targetUri;
	public String targetUsername;
	public String targetPassword;
	public String idField;
	public short pollingInterval = 5;
	public String syncUri;
	public String syncKeyStore;
	public String syncPassword;
	public String mailSmtpHost;
	public String mailSender;
	public String mailRecipients;
	public String mailSubjSuccess;
	public String mailSubjFailure;
	public String mailBodySuccess;
	private Optional<String> lastExceptionMessage = Optional.empty();
	
	public String mruTimestamp;
	public boolean optionSyncOnce;
	public Logger logger;
	
	public static class StatusRecord {
		public String id;
		public String lastModified;
		public int syncedStatus;
		public String syncedTimestamp;
	}
	
	public static class WrappedJsonArray<T> {
		public T[] d;
	}
	
	public static void main(String[] args) throws IOException {
		 TimeZone timeZone = TimeZone.getTimeZone("UTC");
		 TimeZone.setDefault(timeZone);

		String configFile = args.length > 0 ? args[0] : "SimpleSyncService.yaml";
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory()); 
		final SimpleSyncServiceManager mgr = mapper.readValue(new File(configFile), SimpleSyncServiceManager.class);
		mgr.logger = LoggerFactory.getLogger(SimpleSyncServiceManager.class);
		mgr.logger.info("SimpleSyncServiceManager started.");
		mgr.logger.info("Source URI: {}", mgr.sourceUri);
		mgr.logger.info("Target URI: {}", mgr.targetUri);
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> future = executor.scheduleAtFixedRate(mgr::run, 0, mgr.pollingInterval, TimeUnit.MINUTES);
		try {
			future.get(); // need this to retrieve any exceptions
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.getCause().printStackTrace();
		} finally {
			executor.shutdown(); // process will not terminate without this
		}
	}
	
    public Client initSource() throws GeneralSecurityException, IOException {

    	System.setProperty("https.protocols", "TLSv1.2");
        ClientBuilder builder = ClientBuilder.newBuilder()
        		// Set up a HostnameVerifier to allow 'localhost' for debugging purposes
    			.hostnameVerifier(new HostnameVerifier()
        {
    		public boolean verify(String hostname, SSLSession session)
            {
                if (hostname.equals(UriBuilder.fromPath(sourceUri).build().getHost()))
                    return true;
                else if (hostname.equals("localhost"))
                	return true;
                return false;
            }
        });
        
        
        // Use peer certificate authentication
        if (!this.sourceKeyStore.isEmpty()) {
	        KeyStore keystore = KeyStore.getInstance("PKCS12");
	        keystore.load(new FileInputStream(this.sourceKeyStore), this.sourcePassword == null ? null : this.sourcePassword.toCharArray());
        	builder.keyStore(keystore, this.sourcePassword == null ? "" : this.sourcePassword);
        }
        return builder.build();
    }
    
    public Client initTarget() {
        
    	if (this.targetUri.isEmpty()) return null;
    	
        Client client = ClientBuilder.newBuilder()
    			// Set up a HostnameVerifier to work around the lack of SNI support on the production server. 
    			.hostnameVerifier(new HostnameVerifier()
        {
    		public boolean verify(String hostname, SSLSession session)
            {
                if (hostname.equals(UriBuilder.fromPath(targetUri).build().getHost()))
                    return true;
                return false;
            }
        }).build();
        
        // Use HTTP basic authentication
        if (!this.targetUsername.isEmpty()) {
        	HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(this.targetUsername, this.targetPassword);
        	client.register(feature);
        }
        return client;
    }
    
    public Client initSync() throws GeneralSecurityException, IOException {
    	ClientBuilder builder = ClientBuilder.newBuilder()
        		// Set up a HostnameVerifier to allow 'localhost' for debugging purposes
    			.hostnameVerifier(new HostnameVerifier()
        {
    		public boolean verify(String hostname, SSLSession session)
            {
                if (hostname.equals(UriBuilder.fromPath(syncUri).build().getHost()))
                    return true;
                else if (hostname.equals("localhost"))
                	return true;
                return false;
            }
        });
        
        // Use peer certificate authentication
        if (this.syncKeyStore != null) {
	        KeyStore keystore = KeyStore.getInstance("PKCS12");
	        keystore.load(new FileInputStream(this.syncKeyStore), this.syncPassword == null ? null : this.syncPassword.toCharArray());
        	builder.keyStore(keystore, this.syncPassword == null ? "" : this.syncPassword);
        }
        
        return builder.register(JacksonFeature.class).build();
    }
    
	public void run() {

		try {
			System.setProperty("https.protocols", "TLSv1.2");
	        
			Client sourceClient = initSource();
			Client syncClient = initSync();
			
			// The highest sync'ed lastModified time only needs to be retrieved on the first run after startup.
		    if (mruTimestamp == null) {
				mruTimestamp = "0001-01-01T00:00:00.000Z";
				Optional<StatusRecord> maxStatus = getSyncRecords(syncClient)
						.filter(s -> s.syncedStatus != 0)
						.max((s1, s2) -> s1.lastModified.compareTo(s2.lastModified));
				if (maxStatus.isPresent()) mruTimestamp = maxStatus.get().lastModified;
			}
			
		    List<String> keysToRemove = getSyncRecords(syncClient, mruTimestamp)
		    		.map(s -> s.id)
		    		.collect(Collectors.toList());

	        //
		    // reset sync status for modified docs
		    //
	        for (final JsonNode doc : getModifiedDocuments(sourceClient)) {
	        	// get the ID field as specified in the config
	        	JsonNode idNode = doc.get(this.idField);
	        	if (idNode == null) throw new IllegalArgumentException("idField : "+idField);
	        	String id = idNode.asText();
        		
	        	// skip the records that match the mruTimestamp and have already been sync'ed
	        	if (keysToRemove.contains(id)) continue;
	        	
	        	String lastModified = sourceUri.contains("/images") ? 
        				doc.get("ModifiedOn").asText() : // hack because the CDR won't let us rename this field
        				doc.get("lastModified").asText();
        		// OK to update mruTimestamp now since we've already run the query
        		if (lastModified.compareTo(this.mruTimestamp) > 0) this.mruTimestamp = lastModified;
            	
            	// reset sync status
            	StatusRecord syncStatus = new StatusRecord();
    			syncStatus.id = id;
    			syncStatus.lastModified = lastModified;
	        	syncStatus.syncedStatus = 0;
	        	syncStatus.syncedTimestamp = null;
	        	Response response = null;
				try {
	        		javax.ws.rs.client.Invocation.Builder builder = 
        				syncClient.target(this.syncUri).request(MediaType.APPLICATION_JSON_TYPE);
	        		// POST should return 409 if it already exists
	        		// PUT will update lastModified and reset sync status unconditionally
	        		response = optionSyncOnce ? builder.post(Entity.entity(syncStatus, MediaType.APPLICATION_JSON))
	        				: builder.put(Entity.entity(syncStatus, MediaType.APPLICATION_JSON));
		        } catch(ProcessingException e) {
		        	//TODO: refine recoverable error conditions
		        	if (!(e.getCause() instanceof ConnectException)) {
		        		throw e;
		        	}
		        	sendAlert(mailSubjFailure, e.getMessage());
		        	// For recoverable errors, retry at next polling interval instead of throwing exception
		        	return;
		        }
	        	int responseCode = response.getStatus();
	        	if (responseCode == 409) {
	        		// We'll should get an update conflict on a POST when the status record already exists.
	        		continue; 
	        	}
	        	if (responseCode < 200 || responseCode >= 300) {
	        		throw new IOException("unexpected response from sync status PUT: " + responseCode);
	        	}	        	
	        }
	        
	        
	        //
	        // Send data and mark as sync'ed
	        //
			SimpleSyncService service = new SimpleSyncService();
			service.sourceUri = UriBuilder.fromPath(this.sourceUri).build();
			service.setSourceKeyStore(new File(this.sourceKeyStore));
			service.sourcePassword = this.sourcePassword;
			service.setTargetUri(this.targetUri.isEmpty() ? null : UriBuilder.fromPath(this.targetUri).build());
			service.setTargetUsername(this.targetUsername);
			service.targetPassword = this.targetPassword;
			service.setIdField(this.idField);
			service.init();
			
	        List<StatusRecord> syncStatusList = getSyncRecords(syncClient)
					.filter(s -> s.syncedStatus == 0)
					.collect(Collectors.toList());
	        for (StatusRecord syncStatus : syncStatusList) {        	
	        	// Send this entity
	        	service.sourceUri = UriBuilder.fromUri(this.sourceUri).path(syncStatus.id).build();
	        	Response response = null;
				try {
	        		response = service.run();
	        	} catch(ProcessingException e) {
		        	//TODO: refine recoverable error conditions
		        	if (!(e.getCause() instanceof ConnectException)) {
		        		throw e;
		        	}
		        	// For recoverable errors, retry at next polling interval instead of throwing exception
		        	return;
		        }
    			int responseCode = response.getStatus();
    			logger.info("key: {}, response: {}.", syncStatus.id, responseCode);
    			if (responseCode >= 400) {
    				String responseMsg = response.readEntity(String.class);
    				logger.error("Error message from target: {}", responseMsg);
    			
    				if (responseCode == 500) {
    					sendAlert(mailSubjFailure, "Server error from target '" + service.getTargetUri() + "', response: "
    							+ responseMsg);
    				}
    			}
    			
    			// Update sync status
	        	syncStatus.syncedStatus = responseCode;
	        	syncStatus.syncedTimestamp = LocalDateTime.now().toString();
	        	try {
	        		response = syncClient.target(syncUri)
	        			.request(MediaType.APPLICATION_JSON_TYPE)
	        			.put(Entity.entity(syncStatus, MediaType.APPLICATION_JSON));
		        } catch(ProcessingException e) {
		        	//TODO: refine recoverable error conditions
		        	if (!(e.getCause() instanceof ConnectException)) {
		        		throw e;
		        	}
		        	sendAlert(mailSubjFailure, e.getMessage());
		        	// For recoverable errors, retry at next polling interval instead of throwing exception
		        	return;
		        }
	        	responseCode = response.getStatus();
	        	if (responseCode < 200 || responseCode >= 300) {
	        		throw new IOException("unexpected response from sync status PUT: " + responseCode);
	        	}	        	
	        }
		}
		// need to wrap checked exceptions because Runnable implementations can't throw them
		catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (GeneralSecurityException e) {
			throw new SecurityException(e);
		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}

	private JsonNode getModifiedDocuments(Client client) throws IOException {
		// GET
    	Response response = client.target(this.sourceUri).queryParam("starttime", this.mruTimestamp).request().get();
    	int httpCode = response.getStatus();
        if (httpCode < 200 || httpCode >= 300) {
        	throw new IOException("unexpected response from '" + sourceUri + "': " + httpCode);
        }
        String sourceData = response.readEntity(String.class);
        JsonNode jsonRoot = new ObjectMapper().readTree(sourceData);
        if (jsonRoot.size() == 1 && jsonRoot.elements().next().isArray()) {
        	return jsonRoot.elements().next();        	
        }
        return jsonRoot;
	}
	
	private Stream<StatusRecord> getSyncRecords(Client client) throws MessagingException, IOException {
		return getSyncRecords(client, null);
	}
	
	private Stream<StatusRecord> getSyncRecords(Client client, String date) throws MessagingException, IOException {
						
        Response response = null;
        try {
        	UriBuilder syncUriBuilder = UriBuilder.fromUri(syncUri);
        	if (date != null && !date.isEmpty())
        		syncUriBuilder = syncUriBuilder.queryParam("starttime", date);
        	response = client.target(syncUriBuilder.build()).request().get();
        	if (lastExceptionMessage.isPresent()) {
        		lastExceptionMessage = Optional.empty();
        		sendAlert(mailSubjSuccess, mailBodySuccess);
        	}
        } catch(ProcessingException e) {
        	//TODO: refine recoverable error conditions
        	if (!(e.getCause() instanceof ConnectException)) {
        		throw e;
        	}
        	if (!lastExceptionMessage.isPresent() || lastExceptionMessage.get() != e.getMessage()) {
	        	lastExceptionMessage = Optional.of(e.getMessage());
	        	sendAlert(mailSubjFailure, e.getMessage());
        	}
        	// For recoverable errors, retry at next polling interval instead of throwing exception
        	return new ArrayList<StatusRecord>().stream();
        }
        int responseCode = response.getStatus();
        if (responseCode < 200 || responseCode >= 300) {
        	throw new IOException("unexpected response from sync status GET: " + responseCode);
        }
        WrappedJsonArray<StatusRecord> wrapper = response.readEntity(new GenericType<WrappedJsonArray<StatusRecord>>() {});
        
        // filter sync list for synced==false
        Stream<StatusRecord> syncStatusList = Arrays.stream(wrapper.d);
        		//.filter(x -> x.syncedStatus == 0)
        		//.collect(Collectors.toList());
        return syncStatusList;
	}

	private void sendAlert(String subject, String message) throws MessagingException {
    	Properties props = new Properties();
        props.put("mail.smtp.host", mailSmtpHost);
        Session session = Session.getInstance(props, null);
        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(mailSender);
        msg.setRecipients(Message.RecipientType.TO, mailRecipients);
        msg.setSubject(subject);
        msg.setText(message);
        Transport.send(msg);
	}
}
