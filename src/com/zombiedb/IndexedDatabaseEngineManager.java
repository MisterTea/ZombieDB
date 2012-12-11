package com.zombiedb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;

public class IndexedDatabaseEngineManager {
	private TSerializer serializer;
	protected TDeserializer deserializer;
	public Random random = new Random(System.currentTimeMillis());

	private Set<Class<?>> classesToIndex = new HashSet<Class<?>>();
	
	private DatabaseEngine databaseEngine;
	
	public IndexedDatabaseEngineManager(DatabaseEngine databaseEngine) throws IOException {
		this.databaseEngine = databaseEngine;
		TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
		serializer = new TSerializer(protocolFactory);
		deserializer = new TDeserializer(protocolFactory);
		
		classesToIndex.add(Boolean.class);
		classesToIndex.add(Byte.class);
		classesToIndex.add(Character.class);
		classesToIndex.add(Short.class);
		classesToIndex.add(Integer.class);
		classesToIndex.add(Long.class);
		classesToIndex.add(String.class);
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public synchronized void update(TBase thrift) throws IOException {
		try {
			String id = (String)thrift.getClass().getField("id").get(thrift);
			if(id.contains("^")) {
				throw new IOException("'^' character not allowed in id");
			}
			delete(thrift);
			for(int a=0; a<16; a++) {
				TFieldIdEnum f = thrift.fieldForId(a);
				if(f == null || !thrift.isSet(f)) {
					continue;
				}
				Object value = thrift.getFieldValue(thrift.fieldForId(a));
				if(value == null)
					continue;
				if(!classesToIndex .contains(value.getClass())) {
					continue;
				}
				String key = value.toString();
				if(f.getFieldName().equals("id")) {
					// Primary key
					databaseEngine.putBytes(thrift.getClass().getSimpleName() + "_" + f.getFieldName(), key, serializer.serialize(thrift));
				} else {
					// Secondary key
					byte[] currentList = databaseEngine.getBytes(thrift.getClass().getSimpleName() + "_" + f.getFieldName(), key);
					if (currentList == null || currentList.length == 0) {
						// Was empty, create
						String s = new String("^" + id + "^");
						databaseEngine.putBytes(thrift.getClass().getSimpleName() + "_" + f.getFieldName(), key, s.getBytes("UTF-16LE"));
					} else {
						String s = new String(currentList, "UTF-16LE");
						s = s.concat(id + "^");
						databaseEngine.putBytes(thrift.getClass().getSimpleName() + "_" + f.getFieldName(), key, s.getBytes("UTF-16LE"));
					}
				}
			}
			
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T extends TBase, K> void delete(T inMemoryThrift) throws IOException {
		if(inMemoryThrift == null) {
			return;
		}
		String id;
		try {
			id = (String)inMemoryThrift.getClass().getField("id").get(inMemoryThrift);
		} catch (Exception e) {
			throw new IOException(e);
		}
		T staleThrift = (T)get(inMemoryThrift.getClass(), id);
		if(staleThrift == null) {
			return;
		}
		for(int a=0; a<16; a++) {
			TFieldIdEnum f = staleThrift.fieldForId(a);
			if(f == null || !staleThrift.isSet(f)) {
				continue;
			}
			if(f.getFieldName().equals("id")) {
				// Primary key
				if(!databaseEngine.deleteKey(staleThrift.getClass().getSimpleName() + "_" + f.getFieldName(), id)) {
					throw new IOException("Tried to delete an object in an inconsistent state");
				}
				continue;
			}
			Object value = staleThrift.getFieldValue(staleThrift.fieldForId(a));
			if(value == null)
				continue;
			if(!classesToIndex.contains(value.getClass())) {
				continue;
			}
			
			byte[] b = databaseEngine.getBytes(staleThrift.getClass().getSimpleName() + "_" + f.getFieldName(), value.toString());
			if(b == null || b.length == 0) {
				throw new IOException("Tried to delete an object in an inconsistent state");
			}
			
			String original = new String(b, "UTF-16LE");
			String s = original.replace(id + "^", "");
			if(s.equals(original)) {
				throw new IOException("Tried to delete an object in an inconsistent state");
			}
			
			if(s.length()==1) {
				if(!databaseEngine.deleteKey(staleThrift.getClass().getSimpleName() + "_" + f.getFieldName(), value.toString())) {
					throw new IOException("Tried to delete an object in an inconsistent state");
				}
			} else {
				databaseEngine.putBytes(staleThrift.getClass().getSimpleName() + "_" + f.getFieldName(), value.toString(), s.getBytes("UTF-16LE"));
			}
		}
	}

	public synchronized <T extends TBase<?,?>> T get(Class<T> in, String key) throws IOException {
		T emptyThrift;
		try {
			emptyThrift = in.newInstance();
		} catch (Exception e) {
			throw new IOException("Could not create Thrift object");
		}
		try {
			byte[] value = databaseEngine.getBytes(in.getSimpleName() + "_id", key);
			if(value == null) {
				return null;
			}
			deserializer.deserialize(emptyThrift, value);
		} catch (TException e) {
			throw new IOException("Could not deserialize Thrift object");
		}
		return emptyThrift;
	}
	
	public synchronized <T extends TBase<?,?>, K> ArrayList<T> secondaryGet(Class<T> in, String fieldName, K key) throws IOException {
		ArrayList<T> retval = new ArrayList<T>();
		if(key == null) {
			return retval;
		}
		
		if(fieldName.equals("id")) {
			// This is actually a primary key using the secondary key interface
			T t = get(in, (String)key);
			if(t != null) {
				retval.add(t);
			}
			return retval;
		}
		
		byte[] b = databaseEngine.getBytes(in.getSimpleName() + "_" + fieldName, key.toString());
		if(b == null || b.length == 0) {
			return retval;
		}
		
		String[] s = new String(b, "UTF-16LE").split("\\^");
		for(String valueId : s) {
			if(valueId.length()==0) {
				continue;
			}
			retval.add(get(in, valueId));
		}
		return retval;
	}
	
	public synchronized <T extends TBase<?,?>> List<T> getList(Class<T> in, Collection<String> ids) throws IOException {
		List<T> thrifts = new ArrayList<T>();
		
		for(String id : ids) {
			T thrift = get(in, id);
			if (thrift != null) {
				thrifts.add(thrift);
			}
		}
		return thrifts;
	}

	public synchronized <T extends TBase<?,?>> Map<String, T> getAllRows(Class<T> in) throws IOException {
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

	public class ThriftWrapperIterator<T extends TBase<?,?>> implements Iterator<T> {
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

	public <T> int size(Class<T> in) throws IOException {
		return databaseEngine.numValues(in.getSimpleName() + "_id");
	}

	public <T extends TBase<?,?>> ThriftWrapperIterator<T> getValueIterator(Class <T> in) throws IOException {
		return new ThriftWrapperIterator<T>(in, databaseEngine.getValueIterator(in.getSimpleName() + "_id"));
	}
	
	private static final String CHARACTERS = "123456789qwertyuiopasdfghjklzxcvbnm";
	private String generateString(int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = IndexedDatabaseEngineManager.CHARACTERS.charAt(random.nextInt(IndexedDatabaseEngineManager.CHARACTERS.length()));
	    }
	    return new String(text);
	}
}
