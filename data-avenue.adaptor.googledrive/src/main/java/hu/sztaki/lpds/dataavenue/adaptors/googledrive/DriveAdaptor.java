package hu.sztaki.lpds.dataavenue.adaptors.googledrive;

import static hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.v2.users.FullAccount;
import com.dropbox.core.v2.users.DbxUserUsersRequests;

import hu.sztaki.lpds.dataavenue.interfaces.Adaptor;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationField;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationType;
import hu.sztaki.lpds.dataavenue.interfaces.AuthenticationTypeList;
import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.DataAvenueSession;
import hu.sztaki.lpds.dataavenue.interfaces.OperationsEnum;
import hu.sztaki.lpds.dataavenue.interfaces.TransferMonitor;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.CredentialException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.OperationNotSupportedException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.TaskIdException;
import hu.sztaki.lpds.dataavenue.interfaces.exceptions.URIException;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationFieldImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.AuthenticationTypeListImpl;
import hu.sztaki.lpds.dataavenue.interfaces.impl.DefaultURIBaseImpl;



public class DriveAdaptor implements Adaptor {
	
	private static final Logger log = LoggerFactory.getLogger(DriveAdaptor.class);
	
	private String adaptorVersion = "1.0.0"; // default adaptor version
	
	static final String PROTOCOL_PREFIX = "dropbox";
	static final String PROTOCOLS = "dropbox"; 
	static final List<String> APIS = new Vector<String>();
	static final List<String> PROVIDERS = new Vector<String>();


	static final String USERPASS_AUTH = "UserPass";
	
	static final String ACCESS_KEY_CREDENTIAL = "accessKey";
	static final String LEGACY_ACCESS_KEY_CREDENTIAL = "UserID";


	public DriveAdaptor() {
		String PROPERTIES_FILE_NAME = "META-INF/data-avenue-adaptor.properties"; // try to read version number
		try {
			Properties prop = new Properties();
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME); // emits no exception but null returned
			if (in == null) log.warn("Cannot find properties file: " + PROPERTIES_FILE_NAME); 
			else {
				try {
					prop.load(in);
					try { in.close(); } catch (IOException e) {}
					if (prop.get("version") != null) adaptorVersion = (String) prop.get("version");
				} catch (Exception e) { log.warn("Cannot load properties from file: " + PROPERTIES_FILE_NAME); }
			}
		} catch (Throwable e) { log.warn("Cannot read properties file: " + PROPERTIES_FILE_NAME); }
	}
	
	/* adaptor meta information */
	@Override public String getName() { return "Drive Adaptor"; }
	@Override public String getDescription() { return "Drive Adaptor allows of connecting to Google drive storages"; }
	@Override public String getVersion() { return adaptorVersion; }
	
	@Override  public List<String> getSupportedProtocols() {
		List<String> result = new Vector<String>();
		result.add(PROTOCOLS);
		return result;
	}	
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(String protocol) {
		List<OperationsEnum> result = new Vector<OperationsEnum>();
		result.add(LIST); 
		result.add(MKDIR);
		result.add(RMDIR);
		result.add(DELETE); 
		result.add(RENAME);
		result.add(PERMISSIONS);
		result.add(INPUT_STREAM);  
		result.add(OUTPUT_STREAM);
		return result;
	}
	
	@Override public List<OperationsEnum> getSupportedOperationTypes(final URIBase fromURI, final URIBase toURI) {
		return getSupportedOperationTypes(PROTOCOL_PREFIX);
	}
	
	@Override public List<String> getAuthenticationTypes(String protocol) {
		List<String> result = new Vector<String>();
		// for all protocols
		result.add(USERPASS_AUTH);
		return result;
	}
	
	@Override public String getAuthenticationTypeUsage(String protocol,String authenticationType) {
		if (protocol == null || authenticationType == null) throw new IllegalArgumentException("null argument");
		if (USERPASS_AUTH.equals(authenticationType)) return "<b>UserID</b> (access key), <b>UserPass</b> (secret key)";
		return null;
	}

	@Override
	public AuthenticationTypeList getAuthenticationTypeList(String arg0) {
		
		AuthenticationTypeList l = new AuthenticationTypeListImpl();

		AuthenticationType a = new AuthenticationTypeImpl();
		
		a = new AuthenticationTypeImpl();
		a.setType("UserPass");
		a.setDisplayName("authentication");
		
		AuthenticationField f1 = new AuthenticationFieldImpl();
		f1.setKeyName(ACCESS_KEY_CREDENTIAL);
		f1.setDisplayName("Dropbox token");
		a.getFields().add(f1);
		
		l.getAuthenticationTypes().add(a);
		
		return l;
	}
	
	private DbxClientV2 getGoogleDriveClient(URIBase uri, Credentials credentials, DataAvenueSession session) throws OperationException, GeneralSecurityException, CredentialException {
		DriveClient clients = null;
		if (clients == null) {	
			if (credentials == null) throw new CredentialException("No credentials!");
			if (credentials.getCredentialAttribute(ACCESS_KEY_CREDENTIAL) == null) credentials.putCredentialAttribute(ACCESS_KEY_CREDENTIAL, credentials.getCredentialAttribute(LEGACY_ACCESS_KEY_CREDENTIAL)); // legacy // legacy
			
			String accessKey = credentials.getCredentialAttribute(ACCESS_KEY_CREDENTIAL); 
			
			try {
				clients = new DriveClient().withClient(uri,accessKey);
			} catch (IOException x) { throw new OperationException(x); }
		} 
		DbxClientV2 client = clients.get(uri);
		if (client == null) throw new OperationException("APPLICATION ERROR: Cannot create Google Drive client!"); 
		return client; 
	}


	@Override public List<URIBase> list(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException {
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a URL or a directory!");
	
		try {		
			
			DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
			List<URIBase> result = new Vector<URIBase>();
			
			log.info("google drive: " + client.auth());
			log.info("google drive: " + client.fileProperties());
			log.info("google drive: " + client.users().getCurrentAccount().getName());
			
		// Get files and folder metadata from Drop box root directory
				log.info("list path: " +  uri.getPath());
			ListFolderResult res = client.files().listFolder("/"+uri.getPath());
			
			while (true) {
				for (Metadata metadata : res.getEntries()) {
						log.info("google drive: " +  metadata.getName());
					result.add(new DefaultURIBaseImpl(metadata.getName() ));
				}
				
				if (!res.getHasMore()) {
					break;
				}
				res = client.files().listFolderContinue(res.getCursor());
			}					
	
	         return result;
				
		} catch (Throwable e) {
			log.warn("Operation failed!", e);
			throw new OperationException(e);
		}
	}

	@Override public URIBase attributes(final URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("Directory path expected!");
		
			String newFolder = "";
		String asd = uri.getPath();
		for(int i = 1;i < uri.getPath().length();i++){
				if(asd.substring(i-1,i) != "/"	){
					newFolder += asd.substring(i-1,i);
					}
			}

	   try {
		 	DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
		 	
		 	DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl("/"+ newFolder);
	       //uriEntry.setLastModified(client.files().download("/"+ newFolder).getResult().getClientModified());
	       	uriEntry.setSize(client.files().download("/"+ newFolder).getResult().getSize());
	       	return uriEntry;
		} catch (Exception x) {
			throw new OperationException(x);
		}
		
	}
	
	@Override public List<URIBase> attributes(URIBase uri, Credentials credentials, DataAvenueSession session, List <String> subentires) throws URIException, OperationException, CredentialException {
		if (subentires != null && subentires.size() > 0) throw new OperationException("Subentry filtering not supported");
		if (uri.getType() != URIBase.URIType.URL && uri.getType() != URIBase.URIType.DIRECTORY) throw new URIException("URI is not a directory: " + uri.getURI());
	
		List<URIBase> result = new Vector<URIBase>();
	/*	
	   try {
		 	DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
		 	
		 		ListFolderResult res = client.files().listFolder("/"+uri.getPath());
			
			while (true) {
				for (Metadata metadata : res.getEntries()) {
						log.info("google drive: " +  metadata.getName());
	 	DefaultURIBaseImpl uriEntry = new DefaultURIBaseImpl("/"+ metadata + "/");
	       //uriEntry.setLastModified(client.files().download("/"+ newFolder).getResult().getClientModified());
	       	uriEntry.setSize(client.files().download(metadata.getName()).getResult().getSize());
		        result.add(uriEntry);
				}
				
				if (!res.getHasMore()) {
					break;
				}
				res = client.files().listFolderContinue(res.getCursor());
			}	
		 	
		} catch (Exception x) {
			throw new OperationException(x);
		}
*/
		return result;
	}
	
	@Override public void mkdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException	{
		if (!uri.getPath().endsWith(DriveURI.PATH_SEPARATOR)) throw new OperationException("URI must end with /!");
	
		String newFolder = "";
		String asd = uri.getPath();
		for(int i = 1;i < uri.getPath().length();i++){
				if(asd.substring(i-1,i) != "/"	){
					newFolder += asd.substring(i-1,i);
					}
			}

	// Upload file to Drop box
	   try {
		   DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
		   	log.info("testing make dir!: " + newFolder);
		   client.files().createFolderV2(newFolder);
		  //   client.files().createFolderV2("/aaaabbbccc");
				log.info("FILE NAME TO CREATE!!!!: " +  uri.getPath());
	   } catch (Exception e) {
			//throw new Exception("URI must end with /!");
	   }
	   
	}

	@Override public void rmdir(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (!uri.getPath().endsWith(DriveURI.PATH_SEPARATOR)) throw new OperationException("URI must end with /!");
   		    	String newFolder = "";
				String asd = uri.getPath();
				for(int i = 1;i < uri.getPath().length();i++){
						if(asd.substring(i-1,i) != "/"	){
							newFolder += asd.substring(i-1,i);
							}
					} 
	        try {
				     DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
				    	log.info(newFolder);
					client.files().deleteV2(newFolder);
	        } catch (Exception ex) {
	   		   //  System.out.println(ex.getMessage());
	        }
	 }

	@Override public void delete(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {

		    	String newFolder = "";
				String asd = uri.getPath();
				for(int i = 1;i < uri.getPath().length();i++){
						if(asd.substring(i-1,i) != "/"	){
							newFolder += asd.substring(i-1,i);
							}
					} 
	        try {
				     DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
				    	log.info(newFolder);
					client.files().deleteV2(newFolder);
	        } catch (Exception ex) {
	   		   //  System.out.println(ex.getMessage());
	        }
	}

	@Override public void permissions(final URIBase uri, final Credentials credentials, final DataAvenueSession session, final String permissionsString) throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}
	
	@Override public void rename(URIBase uri, String newName, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}
	
	class InputStreamWrapper extends InputStream {
		InputStream is;
		DataAvenueSession session;
		InputStreamWrapper(InputStream is, DataAvenueSession session) { this.is = is; this.session = session;}
		@Override public int read() throws IOException { return is.read(); }
		@Override public int read(byte b[]) throws IOException { return is.read(b); }
		@Override public int read(byte b[], int off, int len) throws IOException { return is.read(b, off, len); }
		@Override public void close() throws IOException {
			is.close();
		}
	}

	@Override public InputStream getInputStream(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file: " + uri.getURI());
		// open FileSystem, don't close
		InputStreamWrapper lol = null;
			String newFolder = "";
				String asd = uri.getPath();
				for(int i = 1;i < uri.getPath().length();i++){
						if(asd.substring(i-1,i) != "/"	){
							newFolder += asd.substring(i-1,i);
							}
					} 
		try {
			DbxClientV2 client = getGoogleDriveClient(uri, credentials, session);
			lol =  new InputStreamWrapper(client.files().download("/"+ newFolder).getInputStream(), session);
	   } catch (Exception ex) {
	   		   //  System.out.println(ex.getMessage());
	   }
	   return lol;
	}
	
	class OutputStreamWrapper extends OutputStream {
		OutputStream os;
		DataAvenueSession session;
		OutputStreamWrapper(OutputStream os, DataAvenueSession session) { this.os = os; this.session = session;}
		@Override public void write(int b) throws IOException {	os.write(b); }
		@Override  public void write(byte b[]) throws IOException { os.write(b); }
		@Override  public void write(byte b[], int off, int len) throws IOException { os.write(b, off, len); }
		@Override  public void flush() throws IOException { os.flush(); }
		@Override public void close() throws IOException {
			os.close();
		}		
	}

	@Override public OutputStream getOutputStream(final URIBase uri, final Credentials credentials, final DataAvenueSession dataAvenueSession, long contentLength) throws URIException, OperationException, CredentialException {
		//if (uri.getType() != URIBase.URIType.FILE) throw new URIException("URI is not a file: " + uri.getURI());e
		OutputStreamWrapper lol = null;
			String newFolder = "";
				String asd = uri.getPath();
				for(int i = 1;i < uri.getPath().length();i++){
						if(asd.substring(i-1,i) != "/"	){
							newFolder += asd.substring(i-1,i);
							}
					} 
		try {
			DbxClientV2 client = getGoogleDriveClient(uri, credentials, dataAvenueSession);
			lol =  new OutputStreamWrapper(client.files().upload(newFolder + "/").getOutputStream(), dataAvenueSession);
	   } catch (Exception ex) {
	   		   //  System.out.println(ex.getMessage());
	   }
	   return lol;
	}

	@Override public void writeFromInputStream(URIBase uri, Credentials credentials, DataAvenueSession session, InputStream inputStream, long contentLength) throws URIException, OperationException, CredentialException, IllegalArgumentException, OperationNotSupportedException {
			// use getOutputStream instead
		throw new OperationNotSupportedException("Not implemented");
	}

	@Override public String copy(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
		throw new OperationException("Not implemented");
	}

	@Override public String move(URIBase fromUri, Credentials fromCredentials, URIBase toUri, Credentials toCredentials, boolean overwrite, TransferMonitor monitor) throws URIException, OperationException, CredentialException {
	  throw new OperationException("Not implemented");
	}

	@Override public void cancel(String id) throws TaskIdException, OperationException {
		throw new OperationException("Not implemented");
	}

	@Override public long getFileSize(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
	throw new OperationException("Not implemented");
	}

	public boolean exists(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return false;
	}
	
	// test if object is readable
	@Override public boolean isReadable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return false;
	}

	@Override public boolean isWritable(URIBase uri, Credentials credentials, DataAvenueSession session) throws URIException, OperationException, CredentialException {
		return false;
	}

	@Override public void shutDown() {
		// no resources to free up
	}
}
