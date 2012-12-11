package com.zombiedb;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public abstract class DatabaseEngine {
	public DatabaseEngine() {}
	
	public abstract void wipeDatabase() throws IOException;
	
	abstract byte[] getBytes(String family, String key) throws IOException;
	
	abstract void putBytes(String family, String key, byte[] value) throws IOException;
	
	abstract boolean containsKey(String family, String s) throws IOException;
	
	abstract boolean deleteKey(String family, String s) throws IOException;
	
	abstract int numValues(String family) throws IOException;
	
	public abstract Set<String> getAllIds(String family) throws IOException;
	
	public abstract Iterator<byte[]> getValueIterator(String family) throws IOException;
	
	public abstract void commit();

	public abstract void destroy();
	
	public abstract void clear(String family) throws IOException;

	public void acquireLock(String family, String key) throws IOException {}

	public void releaseLock(String family, String key) throws IOException {}
}
