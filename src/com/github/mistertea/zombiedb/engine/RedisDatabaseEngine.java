package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import redis.clients.jedis.Jedis;
import redis.clients.util.SafeEncoder;

import com.github.jedis.lock.JedisLock;

public class RedisDatabaseEngine implements DatabaseEngine {

	private Jedis jedis;
	
	private Map<String, byte[]> hashNameBytesMap = new HashMap<String, byte[]>();

	private ConcurrentMap<String,JedisLock> locks = new ConcurrentHashMap<String,JedisLock>();

	public RedisDatabaseEngine(int dbIndex, boolean wipe) throws IOException {
		super();
		jedis = new Jedis("localhost");
		jedis.connect();
		//System.out.println("SELECT RETURNS: " + jedis.select(dbIndex));
		if(wipe) {
			wipeDatabase();
		}
	}

	@Override
	public void acquireLock(String className, String key) throws IOException {
		//System.out.println("LOCKING: " + className + " : " + key);
		boolean acquired;
		try {
			JedisLock lock = new JedisLock(jedis, className + ":" + key);
			acquired = lock.acquire();
			if(!acquired) {
				throw new RuntimeException("Error acquiring lock");
			}
			locks.put(className + ":" + key ,lock);
		} catch (InterruptedException e) {
			throw new RuntimeException("Error acquiring lock");
		}
		//System.out.println("DONE LOCKING: " + className + " : " + key);
	}

	@Override
	public void clear(String family) {
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}

	@Override
	public boolean commit() {
		// No commit in jedis
		return true;
	}

	@Override
	public boolean containsKey(String className, String key) {
		return jedis.hexists(getClassNameBytes(className), SafeEncoder.encode(key));
	}

	@Override
	public void deleteKey(String className, String key) {
		jedis.hdel(getClassNameBytes(className), SafeEncoder.encode(key));
	}
	
	@Override
	public void destroy() {
		jedis.disconnect();
		jedis = null;
	}

	@Override
	public Set<String> getAllIds(String family) {
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}

	@Override
	public byte[] getBytes(String className, String key) {
		return jedis.hget(getClassNameBytes(className), SafeEncoder.encode(key));
	}

	private byte[] getClassNameBytes(String className) {
		byte[] ret = hashNameBytesMap.get(className);
		if(ret == null) {
			ret = SafeEncoder.encode(className);
			hashNameBytesMap.put(className, ret);
		}
		return ret;
	}

	@Override
	public Iterator<byte[]> getValueIterator(String family) {
		return jedis.hgetAll(getClassNameBytes(family)).values().iterator();
	}

	@Override
	public synchronized int numValues(String family) {
		return jedis.hlen(getClassNameBytes(family)).intValue();
	}

	@Override
	public void putBytesBatch(String className, String key, byte[] value) {
		putBytesAtomic(className, key, value);
	}

	@Override
	public void putBytesAtomic(String className, String key, byte[] value) {
		jedis.hset(getClassNameBytes(className), SafeEncoder.encode(key), value);
	}
	
	@Override
	public void releaseLock(String className, String key) throws IOException {
		//System.out.println("UNLOCKING: " + className + " : " + key);
		locks.get(className + ":" + key).release();
		locks.remove(className + ":" + key);
	}

	@Override
	public void wipeDatabase() throws IOException {
		jedis.flushDB();
	}
}
