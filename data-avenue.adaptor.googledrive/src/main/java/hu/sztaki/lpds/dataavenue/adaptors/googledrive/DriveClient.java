package hu.sztaki.lpds.dataavenue.adaptors.googledrive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;

import hu.sztaki.lpds.dataavenue.interfaces.Credentials;
import hu.sztaki.lpds.dataavenue.interfaces.URIBase;

public class DriveClient  {

	private final static Map<String, DbxClientV2> clients = new HashMap<String, DbxClientV2>(); // map: host -> client
	
	
	DriveClient withClient(final URIBase uri, final String access) throws IOException, GeneralSecurityException {
		
		DbxRequestConfig config = new DbxRequestConfig("dropbox/Balintskac");
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		clients.put(hostAndPort, new DbxClientV2(config, access));
		return this;
	}
	
	DbxClientV2 get(final URIBase uri) {
		String hostAndPort = uri.getHost() + (uri.getPort() != null ? ":" + uri.getPort() : "");
		return clients.get(hostAndPort);
	}

}
