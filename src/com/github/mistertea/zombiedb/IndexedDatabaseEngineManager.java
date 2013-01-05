package com.github.mistertea.zombiedb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import com.github.mistertea.zombiedb.engine.DatabaseEngine;
import com.github.mistertea.zombiedb.thrift.TestThrift;

/**
 * The Class IndexedDatabaseEngineManager supports both primary keys adn secondaries indices.  Field in the thrift object with ID 2-16
 * will automatically be indexed if they are a primary type (e.g. string, integer, boolean).
 */
public class IndexedDatabaseEngineManager extends AbstractDatabaseEngineManager {
	private class KeyNames {
		
		TFieldIdEnum primary;
		Set<TFieldIdEnum> secondaries;
		Set<String> secondaryNames = new HashSet<String>();;
		
		public KeyNames(TFieldIdEnum primary, Set<TFieldIdEnum> secondaries) {
			this.primary = primary;
			this.secondaries = secondaries;
			for(TFieldIdEnum secondary : secondaries) {
				secondaryNames.add(secondary.getFieldName());
			}
		}
	}
	
	/** The classes to index. */
	private Set<Byte> classesToIndex = new HashSet<Byte>();
	
	/** The metadata. */
	private Map<String, KeyNames> metadata = new HashMap<String, KeyNames>();
	
	/**
	 * Instantiates a new indexed database engine manager.
	 *
	 * @param databaseEngine the database engine
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public IndexedDatabaseEngineManager(DatabaseEngine databaseEngine) throws IOException {
		super(databaseEngine);
		
		classesToIndex.add(org.apache.thrift.protocol.TType.BOOL);
		classesToIndex.add(org.apache.thrift.protocol.TType.BYTE);
		classesToIndex.add(org.apache.thrift.protocol.TType.ENUM);
		classesToIndex.add(org.apache.thrift.protocol.TType.I16);
		classesToIndex.add(org.apache.thrift.protocol.TType.I32);
		classesToIndex.add(org.apache.thrift.protocol.TType.I64);
		classesToIndex.add(org.apache.thrift.protocol.TType.STRING);
	}
	
	private <F extends TFieldIdEnum, T extends TBase<?, F>> void createMetadataIfNeeded(Class<T> in) throws IOException {
		if (metadata.containsKey(in.getSimpleName())) {
			return;
		}
		T emptyThrift;
		try {
			emptyThrift = in.newInstance();
		} catch (Exception e) {
			throw new IOException("Could not create Thrift object");
		}
		createMetadataIfNeeded(emptyThrift);
	}
	
	private <F extends TFieldIdEnum, T extends TBase<?, F>> void createMetadataIfNeeded(T thrift) throws IOException {
		if (metadata.containsKey(thrift.getClass().getSimpleName())) {
			return;
		}
		
		Map<? extends TFieldIdEnum, FieldMetaData> thriftMetadata = org.apache.thrift.meta_data.FieldMetaData.getStructMetaDataMap(TestThrift.class);
		TFieldIdEnum primary = thrift.fieldForId(1);
		Set<TFieldIdEnum> secondaries = new HashSet<TFieldIdEnum>();
		for(int a=2; a<=16; a++) {
			F f = thrift.fieldForId(a);
			if(f == null) {
				continue;
			}
			FieldMetaData fieldMetaData = thriftMetadata.get(f);
			
			if(!classesToIndex.contains(fieldMetaData.valueMetaData.type)) {
				continue;
			}
			
			secondaries.add(thrift.fieldForId(a));
		}
		metadata.put(thrift.getClass().getSimpleName(), new KeyNames(primary, secondaries));
	}
		
	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#delete(org.apache.thrift.TBase)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <F extends TFieldIdEnum, T extends TBase<?, F>> void delete(T inMemoryThrift) throws IOException {
		if(inMemoryThrift == null) {
			return;
		}
		createMetadataIfNeeded(inMemoryThrift);
		
		// A delete is about to take place, get a consistent view by committing
		databaseEngine.commit();
		
		String id = (String)inMemoryThrift.getFieldValue(inMemoryThrift.fieldForId(1));
		
		T staleThrift = (T) get(inMemoryThrift.getClass(), id);
		if(staleThrift == null) {
			return;
		}
		
		String className = staleThrift.getClass().getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);
		
		{
			// Primary key
			F field = (F) thriftMetaData.primary;
			if (staleThrift.isSet(field) && staleThrift.getFieldValue(field) != null) {
				databaseEngine.deleteKey(className + "_" + field.getFieldName(),
						staleThrift.getFieldValue(field).toString());
			} else {
				throw new IOException("Missing primary key on record: " + staleThrift);
			}
		}
		
		for(TFieldIdEnum abstractField : thriftMetaData.secondaries) {
			// Secondary key(s)
			F field = (F)abstractField;
			if (staleThrift.isSet(field) && staleThrift.getFieldValue(field) != null) {
				String family = className + "_" + field.getFieldName();
				String key = staleThrift.getFieldValue(field).toString();
				byte[] currentList = databaseEngine.getBytes(family, key);

				if(currentList == null || currentList.length == 0) {
					throw new IOException("Tried to delete an object in an inconsistent state: " + currentList);
				}
				
				String original = new String(currentList, "ISO-8859-1");
				String s = original.replace(id + "^", "");
				if(s.equals(original)) {
					throw new IOException("Tried to delete an object in an inconsistent state: " + currentList);
				}
				
				if(s.length()==1) {
					if(!databaseEngine.deleteKey(family, key)) {
						throw new IOException("Tried to delete an object in an inconsistent state: " + currentList);
					}
				} else {
					databaseEngine.putBytes(family, key, s.getBytes("ISO-8859-1"));
				}
			}
		}
		
		// A delete took place and ZombieDB assumes deletes automatically flush, we need to commit the delete
		databaseEngine.commit();
	}
	
	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#get(java.lang.Class, java.lang.String)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> T get(Class<T> in, String key) throws IOException {
		T emptyThrift;
		try {
			emptyThrift = in.newInstance();
		} catch (Exception e) {
			throw new IOException("Could not create Thrift object");
		}
		createMetadataIfNeeded(emptyThrift);
		
		String className = emptyThrift.getClass().getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);
		String family = className + "_" + thriftMetaData.primary.getFieldName();
		
		try {
			byte[] value = databaseEngine.getBytes(family, key);
			if(value == null) {
				return null;
			}
			deserializer.deserialize(emptyThrift, value);
		} catch (TException e) {
			throw new IOException("Could not deserialize Thrift object");
		}
		return emptyThrift;
	}

	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#getValueIterator(java.lang.Class)
	 */
	@Override
	public <F extends TFieldIdEnum, T extends TBase<?, F>> ThriftWrapperIterator<F,T> getValueIterator(Class <T> in) throws IOException {
		return new ThriftWrapperIterator<F,T>(in, deserializer, databaseEngine.getValueIterator(in.getSimpleName() + "_id"));
	}
	
	/**
	 * Secondary get.  Gets a list of thrift objects from a secondary index.  Note that this method returns a list
	 * because, although ids are unique, values of other fields may not be.
	 *
	 * @param in the type to get
	 * @param fieldName the field name of the secondary index to look up.
	 * @param key the key
	 * @return the list of objects found.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>, K> ArrayList<T> secondaryGet(Class<T> in, String fieldName, K key) throws IOException {
		ArrayList<T> retval = new ArrayList<T>();
		if(key == null) {
			return retval;
		}
		createMetadataIfNeeded(in);
		
		String className = in.getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);
		
		if(thriftMetaData.primary.getFieldName().equals(fieldName)) {
			// This is actually a primary key using the secondary key interface
			T t = get(in, (String)key);
			if(t != null) {
				retval.add(t);
			}
			return retval;
		}
		
		if(!thriftMetaData.secondaryNames.contains(fieldName)) {
			throw new IOException("Field name " + fieldName + " is not indexed.");
		}
		
		byte[] b = databaseEngine.getBytes(in.getSimpleName() + "_" + fieldName, key.toString());
		if(b == null || b.length == 0) {
			return retval;
		}
		
		String[] s = new String(b, "ISO-8859-1").split("\\^");
		for(String valueId : s) {
			if(valueId.length()==0) {
				continue;
			}
			retval.add(get(in, valueId));
		}
		return retval;
	}
	
	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#size(java.lang.Class)
	 */
	@Override
	public <F extends TFieldIdEnum, T extends TBase<?, F>> int size(Class<T> in) throws IOException {
		return databaseEngine.numValues(in.getSimpleName() + "_id");
	}

	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#update(org.apache.thrift.TBase)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void update(T thrift) throws IOException {
		createMetadataIfNeeded(thrift);
		try {
			String id = (String)thrift.getFieldValue(thrift.fieldForId(1));
			if(id.contains("^")) {
				throw new IOException("'^' character not allowed in id");
			}
			delete(thrift);

			String className = thrift.getClass().getSimpleName();
			final KeyNames thriftMetaData = metadata.get(className);
			
			{
				// Primary key
				F field = (F) thriftMetaData.primary;
				if (thrift.isSet(field) && thrift.getFieldValue(field) != null) {
					databaseEngine.putBytes(className + "_" + field.getFieldName(),
							thrift.getFieldValue(field).toString(),
							serializer.serialize(thrift));
				} else {
					throw new IOException("Missing primary key on record: " + thrift);
				}
			}
			
			for(TFieldIdEnum abstractField : thriftMetaData.secondaries) {
				// Secondary key(s)
				F field = (F)abstractField;
				if (thrift.isSet(field) && thrift.getFieldValue(field) != null) {
					String family = className + "_" + field.getFieldName();
					String key = thrift.getFieldValue(field).toString();
					byte[] currentList = databaseEngine.getBytes(family, key);
					if (currentList == null || currentList.length == 0) {
						// Was empty, create
						String s = new String("^" + id + "^");
						databaseEngine.putBytes(family, key, s.getBytes("ISO-8859-1"));
					} else {
						String s = new String(currentList, "ISO-8859-1");
						s = s.concat(id + "^");
						databaseEngine.putBytes(family, key, s.getBytes("ISO-8859-1"));
					}
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
