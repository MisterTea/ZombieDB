package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.util.SafeEncoder;

import com.github.jedis.lock.JedisLock;

public class RedisDatabaseEngine extends DatabaseEngine {

	private Jedis jedis;
	
	private Map<String, byte[]> hashNameBytesMap = new HashMap<String, byte[]>();

	public RedisDatabaseEngine(int dbIndex) throws IOException {
		super();
		jedis = new Jedis("localhost");
		jedis.connect();
		System.out.println("SELECT RETURNS: " + jedis.select(dbIndex));
	}

	@Override
	public void acquireLock(String className, String key) throws IOException {
		boolean acquired;
		try {
			acquired = new JedisLock(className + ":" + key).acquire();
		} catch (InterruptedException e) {
			throw new RuntimeException("Error acquiring lock");
		}
		if(!acquired) {
			throw new RuntimeException("Error acquiring lock");
		}
	}

	@Override
	public void clear(String family) {
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}

	@Override
	public void commit() {
		// No commit in jedis
	}

	@Override
	public boolean containsKey(String className, String key) {
		return jedis.hexists(getClassNameBytes(className), SafeEncoder.encode(key));
	}

	@Override
	public boolean deleteKey(String className, String key) {
		return jedis.hdel(getClassNameBytes(className), SafeEncoder.encode(key))==1;
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
	public void putBytes(String className, String key, byte[] value) {
		jedis.hset(getClassNameBytes(className), SafeEncoder.encode(key), value);
	}

	@Override
	public void releaseLock(String className, String key) throws IOException {
		new JedisLock(className + ":" + key).release();
	}

	@Override
	public void wipeDatabase() throws IOException {
		jedis.flushDB();
	}
}
