package org.vai.vari.pbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class SimpleSyncServiceManager {
	
	public URI sourceUri;
	public String sourceKeyStore;
	public String sourcePassword;
	public URI targetUri;
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
			service.sourceUri = this.sourceUri;
			service.setSourceKeyStore(new File(this.sourceKeyStore));
			service.sourcePassword = this.sourcePassword;
			service.setTargetUri(this.targetUri);
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
	        
	        Client client = builder.build();
	        Response syncResponse = null;
	        try {
	        	syncResponse = client.target(syncUri).request().get();
	        	if (lastExceptionMessage.isPresent()) {
	        		lastExceptionMessage = Optional.empty();
		        	Properties props = new Properties();
		            props.put("mail.smtp.host", mailSmtpHost);
		            Session session = Session.getInstance(props, null);
		            MimeMessage msg = new MimeMessage(session);
		            msg.setFrom(mailSender);
		            msg.setRecipients(Message.RecipientType.TO, mailRecipients);
		            msg.setSubject(mailSubjSuccess);
		            msg.setText(mailBodySuccess);
		            Transport.send(msg);
	        	}
	        } catch(ProcessingException e) {
	        	//TODO: refine recoverable error conditions
	        	if (!(e.getCause() instanceof ConnectException)) {
	        		throw e;
	        	}
	        	if (!lastExceptionMessage.isPresent() || lastExceptionMessage.get() != e.getMessage()) {
		        	lastExceptionMessage = Optional.of(e.getMessage());
		        	Properties props = new Properties();
		            props.put("mail.smtp.host", mailSmtpHost);
		            Session session = Session.getInstance(props, null);
		            MimeMessage msg = new MimeMessage(session);
		            msg.setFrom(mailSender);
		            msg.setRecipients(Message.RecipientType.TO, mailRecipients);
		            msg.setSubject(mailSubjFailure);
		            msg.setText(e.getMessage());
		            Transport.send(msg);
	        	}
	        	// For recoverable errors, retry at next polling interval instead of throwing exception
	        	return;
	        }
	        int httpCode = syncResponse.getStatus();
	        if (httpCode < 200 || httpCode >= 300) {
	        	throw new IOException("unexpected response from sync info: " + httpCode);
	        }
	        String syncData = syncResponse.readEntity(String.class);
	        JsonNode jsonRoot = new ObjectMapper().readTree(syncData);
	    	if (jsonRoot.size() == 1 && jsonRoot.elements().next().isArray()) {
	    		for (final JsonNode objNode : jsonRoot.elements().next()) {
	    			String id = objNode.get(0).textValue();
	    			service.sourceUri = this.sourceUri.resolve(id);
	    			service.run();
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
}
