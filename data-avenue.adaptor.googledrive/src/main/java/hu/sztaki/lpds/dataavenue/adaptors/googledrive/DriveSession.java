package hu.sztaki.lpds.dataavenue.adaptors.googledrive;

import hu.sztaki.lpds.dataavenue.interfaces.CloseableSessionObject;

public class DriveSession implements CloseableSessionObject {
	
	//private static final Logger log = LoggerFactory.getLogger(JcloudsSession.class);
	//private final Map<String, BlobStoreContext> sessions = new ConcurrentHashMap<String, BlobStoreContext>(); // map: host -> blobstore context
	
	
	/*void put(final String hostAndPort, final BlobStoreContext context) {
		sessions.put(hostAndPort, context);
	}
	
	DirveSession get(final String hostAndPort) {
		return sessions.get(hostAndPort);
	}*/
	
	@Override
	public void close() {

	}
}