package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public abstract class DatabaseEngine {
	public DatabaseEngine() {}
	
	public void acquireLock(String family, String key) throws IOException {}
	
	public abstract void clear(String family) throws IOException;
	
	public abstract void commit() throws IOException;
	
	public abstract boolean containsKey(String family, String s) throws IOException;
	
	public abstract boolean deleteKey(String family, String s) throws IOException;
	
	public abstract void destroy();
	
	public abstract Set<String> getAllIds(String family) throws IOException;
	
	public abstract byte[] getBytes(String family, String key) throws IOException;
	
	public abstract Iterator<byte[]> getValueIterator(String family) throws IOException;

	public abstract int numValues(String family) throws IOException;
	
	public abstract void putBytes(String family, String key, byte[] value) throws IOException;

	public void releaseLock(String family, String key) throws IOException {}

	public abstract void wipeDatabase() throws IOException;
}
