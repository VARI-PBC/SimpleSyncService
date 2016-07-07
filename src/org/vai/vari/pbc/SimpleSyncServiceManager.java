package org.vai.vari.pbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.jackson.JacksonFeature;
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
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory()); 
		final SimpleSyncServiceManager mgr = mapper.readValue(new File("SimpleSyncService.yaml"), SimpleSyncServiceManager.class);
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
	
	public void run() {

		try {
			SimpleSyncService service = new SimpleSyncService();
			service.sourceUri = UriBuilder.fromPath(this.sourceUri).build();
			service.setSourceKeyStore(new File(this.sourceKeyStore));
			service.sourcePassword = this.sourcePassword;
			service.setTargetUri(UriBuilder.fromPath(this.targetUri).build());
			service.setTargetUsername(this.targetUsername);
			service.targetPassword = this.targetPassword;
			service.setIdField(this.idField);
			service.init();
			
			System.setProperty("https.protocols", "TLSv1.2");
	        ClientBuilder builder = ClientBuilder.newBuilder();
	        
	        // Use peer certificate authentication
	        if (syncKeyStore != null) {
		        KeyStore keystore = KeyStore.getInstance("PKCS12");
		        keystore.load(new FileInputStream(syncKeyStore), syncPassword == null ? null : syncPassword.toCharArray());
	        	builder.keyStore(keystore, syncPassword == null ? "" : syncPassword);
	        }
	        
	        Client client = builder.register(JacksonFeature.class).build();
	        Response response = null;
	        try {
	        	response = client.target(syncUri).request().get();
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
	        	return;
	        }
	        int responseCode = response.getStatus();
	        if (responseCode < 200 || responseCode >= 300) {
	        	throw new IOException("unexpected response from sync status GET: " + responseCode);
	        }
	        WrappedJsonArray<StatusRecord> wrapper = response.readEntity(new GenericType<WrappedJsonArray<StatusRecord>>() {});
	        
	        // filter sync list for synced==false
	        List<StatusRecord> syncStatusList = Arrays.stream(wrapper.d)
	        		.filter(x -> x.syncedStatus == 0)
	        		.collect(Collectors.toList());

	        // Send data and mark as synced
	        for (StatusRecord syncStatus : syncStatusList) {
	        	// Send this entity
	        	service.sourceUri = UriBuilder.fromUri(this.sourceUri).path(syncStatus.id).build();
    			int httpCode = service.run();
    			
    			// Update sync status
	        	syncStatus.syncedStatus = httpCode;
	        	syncStatus.syncedTimestamp = LocalDateTime.now().toString();
	        	response = client.target(syncUri)
	        			.request(MediaType.APPLICATION_JSON_TYPE)
	        			.put(Entity.entity(syncStatus, MediaType.APPLICATION_JSON));
	        	responseCode = response.getStatus();
	        	if (responseCode == 409) {
	        		// An update conflict means we should not mark it as sync'ed so that 
	        		// we'll send it again on the next polling interval.
	        		continue; 
	        	}
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
