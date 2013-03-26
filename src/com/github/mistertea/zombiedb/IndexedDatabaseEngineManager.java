package com.github.mistertea.zombiedb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import com.github.mistertea.zombiedb.engine.DatabaseEngine;

/**
 * The Class IndexedDatabaseEngineManager supports both primary keys adn
 * secondaries indices. Field in the thrift object with ID 2-16 will
 * automatically be indexed if they are a primary type (e.g. string, integer,
 * boolean).
 */
public class IndexedDatabaseEngineManager extends AbstractDatabaseEngineManager {
	private final static Logger logger = Logger
			.getLogger(IndexedDatabaseEngineManager.class.getName());

	private class KeyNames {

		TFieldIdEnum primary;
		// These must be tree sets so the order is preserved when iterating over
		// all objects. This ensures
		// that field/row pairs are locked in the same order.
		TreeSet<TFieldIdEnum> secondaries;
		TreeSet<String> secondaryNames = new TreeSet<String>();;

		public KeyNames(TFieldIdEnum primary, TreeSet<TFieldIdEnum> secondaries) {
			this.primary = primary;
			this.secondaries = secondaries;
			for (TFieldIdEnum secondary : secondaries) {
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
	 * @param databaseEngine
	 *            the database engine
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public IndexedDatabaseEngineManager(DatabaseEngine databaseEngine)
			throws IOException {
		super(databaseEngine);

		classesToIndex.add(org.apache.thrift.protocol.TType.BOOL);
		classesToIndex.add(org.apache.thrift.protocol.TType.BYTE);
		classesToIndex.add(org.apache.thrift.protocol.TType.ENUM);
		classesToIndex.add(org.apache.thrift.protocol.TType.I16);
		classesToIndex.add(org.apache.thrift.protocol.TType.I32);
		classesToIndex.add(org.apache.thrift.protocol.TType.I64);
		classesToIndex.add(org.apache.thrift.protocol.TType.STRING);
	}

	private <F extends TFieldIdEnum, T extends TBase<?, F>> void createMetadataIfNeeded(
			Class<T> in) throws IOException {
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

	private <F extends TFieldIdEnum, T extends TBase<?, F>> void createMetadataIfNeeded(
			T thrift) throws IOException {
		if (metadata.containsKey(thrift.getClass().getSimpleName())) {
			return;
		}

		Map<? extends TFieldIdEnum, FieldMetaData> thriftMetadata = org.apache.thrift.meta_data.FieldMetaData
				.getStructMetaDataMap(thrift.getClass());
		TFieldIdEnum primary = thrift.fieldForId(1);
		TreeSet<TFieldIdEnum> secondaries = new TreeSet<TFieldIdEnum>();
		for (int a = 2; a <= 16; a++) {
			F f = thrift.fieldForId(a);
			if (f == null) {
				continue;
			}
			FieldMetaData fieldMetaData = thriftMetadata.get(f);

			if (!classesToIndex.contains(fieldMetaData.valueMetaData.type)) {
				continue;
			}

			secondaries.add(thrift.fieldForId(a));
		}
		metadata.put(thrift.getClass().getSimpleName(), new KeyNames(primary,
				secondaries));
	}

	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#delete(org.apache.thrift.TBase)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void delete(
			T inMemoryThrift) throws IOException {
		if (inMemoryThrift == null) {
			return;
		}
		createMetadataIfNeeded(inMemoryThrift);

		String id = (String) inMemoryThrift.getFieldValue(inMemoryThrift
				.fieldForId(1));

		T staleThrift = (T) getAndLockPrimaryKey(inMemoryThrift.getClass(), id);
		try {
			if (staleThrift == null) {
				return;
			}
			deleteNoPrimaryLock(staleThrift);
		} finally {
			releasePrimaryKeyLock(inMemoryThrift.getClass(), id);
		}
	}

	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> Set<String> getAllKeys(
			Class<T> in) throws IOException {
		createMetadataIfNeeded(in);
		final KeyNames thriftMetaData = metadata.get(in.getSimpleName());
		return databaseEngine.getAllIds(in.getSimpleName() + "_"
				+ thriftMetaData.primary.getFieldName());
	}

	@SuppressWarnings("unchecked")
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> void deleteNoPrimaryLock(
			T staleThrift) throws IOException {
		String id = (String) staleThrift.getFieldValue(staleThrift
				.fieldForId(1));
		lockSecondaryKeys(staleThrift);
		try {
			String className = staleThrift.getClass().getSimpleName();
			final KeyNames thriftMetaData = metadata.get(className);

			for (TFieldIdEnum abstractField : thriftMetaData.secondaries) {
				// Secondary key(s)
				F field = (F) abstractField;
				if (staleThrift.getFieldValue(field) != null) {
					String family = className + "_" + field.getFieldName();
					String key = staleThrift.getFieldValue(field).toString();
					byte[] currentList = databaseEngine.getBytes(family, key);

					if (currentList == null || currentList.length == 0) {
						logger.severe("Tried to delete an object in an inconsistent state: "
								+ family + " : " + key + " : " + currentList);
						continue;
					}

					String original = new String(currentList, "ISO-8859-1");
					String s = original.replace(id + "^", "");
					if (s.equals(original)) {
						logger.severe("Tried to delete an object in an inconsistent state: "
								+ family + " : " + key + " : " + currentList);
						continue;
					}

					if (s.length() == 1) {
						if (!databaseEngine.containsKey(family, key)) {
							throw new IOException(
									"Tried to delete an object in an inconsistent state: "
											+ currentList);
						}
						databaseEngine.deleteKey(family, key);
					} else {
						databaseEngine.putBytesBatch(family, key,
								s.getBytes("ISO-8859-1"));
					}
				}
			}

			{
				// Primary key
				F field = (F) thriftMetaData.primary;
				if (staleThrift.getFieldValue(field) != null) {
					databaseEngine.deleteKey(
							className + "_" + field.getFieldName(), staleThrift
									.getFieldValue(field).toString());
				} else {
					throw new IOException("Missing primary key on record: "
							+ staleThrift);
				}
			}

			databaseEngine.commit();

		} finally {
			releaseSecondaryKeyLocks(staleThrift);
		}
	}

	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#get(java.lang.Class,
	 *      java.lang.String)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> T get(
			Class<T> in, String key) throws IOException {
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
			if (value == null) {
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
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> ThriftWrapperIterator<F, T> getValueIterator(
			Class<T> in) throws IOException {
		return new ThriftWrapperIterator<F, T>(in, deserializer,
				databaseEngine.getValueIterator(in.getSimpleName() + "_id"));
	}

	/**
	 * Secondary get. Gets a list of thrift objects from a secondary index. Note
	 * that this method returns a list because, although ids are unique, values
	 * of other fields may not be.
	 * 
	 * @param in
	 *            the type to get
	 * @param fieldName
	 *            the field name of the secondary index to look up.
	 * @param key
	 *            the key
	 * @return the list of objects found.
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>, K> ArrayList<T> secondaryGet(
			Class<T> in, String fieldName, K key) throws IOException {
		ArrayList<T> retval = new ArrayList<T>();
		if (key == null) {
			return retval;
		}
		createMetadataIfNeeded(in);

		String className = in.getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);

		if (thriftMetaData.primary.getFieldName().equals(fieldName)) {
			// This is actually a primary key using the secondary key interface
			T t = get(in, (String) key);
			if (t != null) {
				retval.add(t);
			}
			return retval;
		}

		if (!thriftMetaData.secondaryNames.contains(fieldName)) {
			throw new IOException("Field name " + fieldName
					+ " is not indexed.");
		}

		byte[] b = databaseEngine.getBytes(
				in.getSimpleName() + "_" + fieldName, key.toString());
		if (b == null || b.length == 0) {
			return retval;
		}

		String[] s = new String(b, "ISO-8859-1").split("\\^");
		for (String valueId : s) {
			if (valueId.length() == 0) {
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
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> int size(Class<T> in)
			throws IOException {
		return databaseEngine.numValues(in.getSimpleName() + "_id");
	}

	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#upsertNonAtomic(org.apache.thrift.TBase)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void upsertNonAtomic(
			T thrift) throws IOException {
		throw new IOException(
				"Non-Atomic upserts not supported when using indexing");
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void upsert(T thrift)
			throws IOException {
		createMetadataIfNeeded(thrift);
		try {
			String id = (String) thrift.getFieldValue(thrift.fieldForId(1));
			if (id.contains("^")) {
				throw new IOException("'^' character not allowed in id");
			}
			T staleThrift = (T) getAndLockPrimaryKey(thrift.getClass(), id);
			try {
				upsertNoPrimaryLock(staleThrift, thrift);
			} finally {
				releasePrimaryKeyLock(thrift.getClass(), id);
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
		databaseEngine.commit();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> void upsertNoPrimaryLock(
			T staleThrift, T thrift) throws IOException {
		String id = (String) thrift.getFieldValue(thrift.fieldForId(1));
		if (staleThrift != null) {
			deleteNoPrimaryLock(staleThrift);
		}
		lockSecondaryKeys(thrift);
		try {

			String className = thrift.getClass().getSimpleName();
			final KeyNames thriftMetaData = metadata.get(className);

			{
				// Primary key
				F field = (F) thriftMetaData.primary;
				if (thrift.getFieldValue(field) != null) {
					databaseEngine.putBytesBatch(
							className + "_" + field.getFieldName(), thrift
									.getFieldValue(field).toString(),
							serializer.serialize(thrift));
				} else {
					throw new IOException("Missing primary key on record: "
							+ thrift);
				}
			}

			for (TFieldIdEnum abstractField : thriftMetaData.secondaries) {
				// Secondary key(s)
				F field = (F) abstractField;
				if (thrift.getFieldValue(field) != null) {
					String family = className + "_" + field.getFieldName();
					String key = thrift.getFieldValue(field).toString();
					byte[] currentList = databaseEngine.getBytes(family, key);
					if (currentList == null || currentList.length == 0) {
						// Was empty, create
						String s = new String("^" + id + "^");
						databaseEngine.putBytesBatch(family, key,
								s.getBytes("ISO-8859-1"));
					} else {
						String s = new String(currentList, "ISO-8859-1");
						s = s.concat(id + "^");
						databaseEngine.putBytesBatch(family, key,
								s.getBytes("ISO-8859-1"));
					}
				}
			}

			databaseEngine.commit();
		} catch (TException e) {
			throw new IOException(e);
		} finally {
			releaseSecondaryKeyLocks(thrift);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> T getAndLockPrimaryKey(
			Class<T> in, String id) throws IOException {
		createMetadataIfNeeded(in);
		String className = in.getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);

		// Primary key
		F field = (F) thriftMetaData.primary;
		databaseEngine.acquireLock(className + "_" + field.getFieldName(), id);
		return get(in, id);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> void releasePrimaryKeyLock(
			Class<T> in, String id) throws IOException {
		createMetadataIfNeeded(in);
		String className = in.getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);

		// Primary key
		F field = (F) thriftMetaData.primary;
		databaseEngine.releaseLock(className + "_" + field.getFieldName(), id);
	}

	@SuppressWarnings("unchecked")
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> void lockSecondaryKeys(
			T thrift) throws IOException {
		createMetadataIfNeeded(thrift);
		String className = thrift.getClass().getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);

		for (TFieldIdEnum abstractField : thriftMetaData.secondaries) {
			// Secondary key(s)
			F field = (F) abstractField;
			if (thrift.getFieldValue(field) != null) {
				String family = className + "_" + field.getFieldName();
				String key = thrift.getFieldValue(field).toString();
				databaseEngine.acquireLock(family, key);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> void releaseSecondaryKeyLocks(
			T thrift) throws IOException {
		createMetadataIfNeeded(thrift);
		String className = thrift.getClass().getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);

		for (TFieldIdEnum abstractField : thriftMetaData.secondaries) {
			// Secondary key(s)
			F field = (F) abstractField;
			if (thrift.getFieldValue(field) != null) {
				String family = className + "_" + field.getFieldName();
				String key = thrift.getFieldValue(field).toString();
				databaseEngine.releaseLock(family, key);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void register(
			Class<T> thriftClass) throws IOException {
		createMetadataIfNeeded(thriftClass);
		getAndLockPrimaryKey(thriftClass, "null");
		releasePrimaryKeyLock(thriftClass, "null");

		String className = thriftClass.getSimpleName();
		final KeyNames thriftMetaData = metadata.get(className);

		for (TFieldIdEnum abstractField : thriftMetaData.secondaries) {
			// Secondary key(s)
			F field = (F) abstractField;
			String family = className + "_" + field.getFieldName();
			databaseEngine.acquireLock(family, "null");
			databaseEngine.releaseLock(family, "null");
		}

		logger.info("Registered " + thriftClass.getSimpleName());
	}
}
