package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public interface DatabaseEngine {
	public void acquireLock(String family, String key) throws IOException;
	
	public void clear(String family) throws IOException;
	
	public boolean commit() throws IOException;
	
	public boolean containsKey(String family, String s) throws IOException;
	
	public void deleteKey(String family, String s) throws IOException;
	
	public void destroy() throws IOException;
	
	public Set<String> getAllIds(String family) throws IOException;
	
	public byte[] getBytes(String family, String key) throws IOException;
	
	public Iterator<byte[]> getValueIterator(String family) throws IOException;

	public int numValues(String family) throws IOException;
	
	public void putBytesBatch(String family, String key, byte[] value) throws IOException;

	public void putBytesAtomic(String family, String key, byte[] value) throws IOException;
	
	public void releaseLock(String family, String key) throws IOException;

	public void wipeDatabase() throws IOException;
}
