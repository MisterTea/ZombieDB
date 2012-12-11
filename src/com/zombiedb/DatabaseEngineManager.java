package com.zombiedb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class DatabaseEngineManager {
	private TSerializer serializer;
	protected TDeserializer deserializer;
	public Random random = new Random(System.currentTimeMillis());

	private DatabaseEngine databaseEngine;
	
	public DatabaseEngineManager(DatabaseEngine databaseEngine) throws IOException {
		this.databaseEngine = databaseEngine;
		TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
		serializer = new TSerializer(protocolFactory);
		deserializer = new TDeserializer(protocolFactory);
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized void create(TBase thrift) throws IOException {
		String prevKey;
		try {
			prevKey = (String)thrift.getClass().getField("id").get(thrift);
		} catch (Exception e) {
			throw new IOException(e);
		}
		if(prevKey != null) {
			throw new IOException("Tried to autogenerate a key for an object that already had a key");
		}
		
		String s = null;
		do {
			s = generateString(16);
		} while(databaseEngine.containsKey(thrift.getClass().getSimpleName(), s));
		
		try {
			thrift.getClass().getField("id").set(thrift, s); //Use reflection to inject the id into the object
			update(thrift);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@SuppressWarnings("rawtypes")
	public synchronized void createWithId(TBase thrift) throws IOException {
		try {
			String s = (String)thrift.getClass().getField("id").get(thrift);
			if(databaseEngine.containsKey(thrift.getClass().getSimpleName(), s)) {
				throw new IOException("Tried to create a new record with a forced duplicate ID: " + s);
			}
			
			update(thrift);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized void update(TBase thrift) throws IOException {
		try {
			String s = (String)thrift.getClass().getField("id").get(thrift);
			
			databaseEngine.putBytes(thrift.getClass().getSimpleName(), s, serializer.serialize(thrift));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized <T> T get(Class<T> in, String id) throws IOException {
		T emptyThrift;
		try {
			emptyThrift = in.newInstance();
		} catch (Exception e) {
			throw new IOException("Could not create Thrift object");
		}
		try {
			byte[] value = databaseEngine.getBytes(in.getSimpleName(), id);
			if(value == null) {
				return null;
			}
			deserializer.deserialize((TBase) emptyThrift, value);
		} catch (TException e) {
			throw new IOException("Could not deserialize Thrift object");
		}
		return emptyThrift;
	}
	
	@SuppressWarnings("rawtypes")
	public synchronized <T extends TBase> List<T> getList(Class<T> in, Collection<String> ids) throws IOException {
		List<T> thrifts = new ArrayList<T>();
		
		for(String id : ids) {
			T thrift = get(in, id);
			if (thrift != null) {
				thrifts.add(thrift);
			}
		}
		return thrifts;
	}

	@SuppressWarnings("rawtypes")
	public synchronized <T extends TBase> Map<String, T> getAllRows(Class<T> in) throws IOException {
		Iterator<T> it = new ThriftWrapperIterator<T>(in, databaseEngine.getValueIterator(in.getSimpleName()));
		Map<String, T> out = new HashMap<String, T>();
		while(it.hasNext()) {
			T t = it.next();
			String id;
			try {
				id = (String)in.getField("id").get(t);
			} catch (Exception e) {
				throw new IOException(e);
			}
			out.put(id, t);
		}
		return out;
	}

	@SuppressWarnings("rawtypes")
	public class ThriftWrapperIterator<T extends TBase> implements Iterator<T> {
		private Class<T> type;
		private Iterator<byte[]> byteIterator;

		public ThriftWrapperIterator(Class<T> type, Iterator<byte[]> byteIterator) {
			this.type = type;
			this.byteIterator = byteIterator;
		}

		@Override
		public boolean hasNext() {
			return byteIterator.hasNext();
		}

		@Override
		public T next() {
			try {
				T emptyThrift = type.newInstance();
				deserializer.deserialize(emptyThrift, byteIterator.next());
				return emptyThrift;
			} catch (TException e) {
				e.printStackTrace();
				return null;
			} catch (InstantiationException e) {
				e.printStackTrace();
				return null;
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public void remove() {
			byteIterator.remove();
		}
		
	}

	public <T> void deleteFromId(Class<T> in, String id) throws IOException {
		databaseEngine.deleteKey(in.getSimpleName(), id);
	}

	public <T> int size(Class<T> in) throws IOException {
		return databaseEngine.numValues(in.getSimpleName());
	}
	
	@SuppressWarnings("rawtypes")
	public <T extends TBase> ThriftWrapperIterator<T> getValueIterator(Class <T> in) throws IOException {
		return new ThriftWrapperIterator<T>(in, databaseEngine.getValueIterator(in.getSimpleName()));
	}

	private static final String CHARACTERS = "123456789qwertyuiopasdfghjklzxcvbnm";
	private String generateString(int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = DatabaseEngineManager.CHARACTERS.charAt(random.nextInt(DatabaseEngineManager.CHARACTERS.length()));
	    }
	    return new String(text);
	}

	public void commit() {
		databaseEngine.commit();
	}

	public <T> Set<String> getAllKeys(Class<T> in) throws IOException {
		return databaseEngine.getAllIds(in.getSimpleName());
	}

	public void destroy() {
		databaseEngine.destroy();
	}

	@SuppressWarnings("rawtypes")
	public <T extends TBase> void clear(Class<T> in) throws IOException {
		databaseEngine.clear(in.getSimpleName());
	}
}
