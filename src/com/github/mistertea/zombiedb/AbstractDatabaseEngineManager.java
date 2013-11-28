package com.github.mistertea.zombiedb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.lang3.ClassUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;

import au.com.bytecode.opencsv.CSVWriter;

import com.github.mistertea.zombiedb.engine.DatabaseEngine;

/**
 * The Class AbstractDatabaseEngineManager is a base class for both database
 * engines.
 */
public abstract class AbstractDatabaseEngineManager {

	protected TSerializer serializer;
	protected TDeserializer deserializer;
	protected Random random = new Random(System.currentTimeMillis());
	protected DatabaseEngine databaseEngine;
	private TSerializer jsonSerializer;
	private TDeserializer jsonDeserializer;
	private static final String CHARACTERS = "123456789qwertyuiopasdfghjklzxcvbnm";

	/**
	 * Instantiates a new abstract database engine manager.
	 * 
	 * @param databaseEngine
	 *            the database engine backing this manager
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public AbstractDatabaseEngineManager(DatabaseEngine databaseEngine)
			throws IOException {
		this.databaseEngine = databaseEngine;
		TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
		serializer = new TSerializer(protocolFactory);
		deserializer = new TDeserializer(protocolFactory);

		TJSONProtocol.Factory jsonProtocolFactory = new TJSONProtocol.Factory();
		jsonSerializer = new TSerializer(jsonProtocolFactory);
		jsonDeserializer = new TDeserializer(jsonProtocolFactory);
	}

	/**
	 * Removes all objects of type T from the database.
	 * 
	 * @param in
	 *            the class to clear
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void clear(
			Class<T> in) throws IOException {
		while (true) {
			Set<String> keys = getAllKeys(in);
			if (keys.isEmpty()) {
				return;
			}
			for (String key : keys) {
				deleteFromId(in, key);
			}
		}
	}

	/**
	 * Commits any pending upserts to the database. Note that some database
	 * engines will flush changes to the database even without a commit.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @return True if the commit succeeded, false if it failed.
	 */
	public synchronized boolean commit() throws IOException {
		return databaseEngine.commit();
	}

	/**
	 * Creates a new entry in the database for a thrift object. Note that this
	 * method expects the object to have no id. For objects that already have a
	 * unique id, use {@link #createWithId(TBase) createWithId}. Create
	 * operations are atomic.
	 * 
	 * @param thrift
	 *            the object to create
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("unchecked")
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void create(
			T thrift) throws IOException {
		String prevKey = (String) thrift.getFieldValue(thrift.fieldForId(1));
		if (prevKey != null && prevKey.length() > 0) {
			throw new IOException(
					"Tried to autogenerate a key for an object that already had a key");
		}

		String newId = null;
		while (true) {
			newId = generateString(16);
			thrift.setFieldValue(thrift.fieldForId(1), newId);
			T existingThrift = (T) getAndLockPrimaryKey(thrift.getClass(),
					newId);
			try {
				if (existingThrift == null) {
					upsertNoPrimaryLock(existingThrift, thrift);
					return;
				}
			} finally {
				releasePrimaryKeyLock(thrift.getClass(), newId);
			}
		}
	}

	protected abstract <F extends TFieldIdEnum, T extends TBase<?, F>> T getAndLockPrimaryKey(
			Class<T> in, String key) throws IOException;

	protected abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void releasePrimaryKeyLock(
			Class<T> in, String key) throws IOException;

	/**
	 * Creates a new entry in the database for a thrift object. Note that this
	 * method expects the object to have an id. For objects that do not have a
	 * unique id populated, use {@link #create(TBase) create}. Create operations
	 * are atomic.
	 * 
	 * @param thrift
	 *            the object to create
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @return False if an object with the same ID existed, true otherwise.
	 */
	@SuppressWarnings("unchecked")
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> boolean createWithId(
			T thrift) throws IOException {
		String id = (String) thrift.getFieldValue(thrift.fieldForId(1));
		T existingThrift = (T) getAndLockPrimaryKey(thrift.getClass(), id);
		try {
			if (existingThrift != null) {
				return false;
			}
			upsertNoPrimaryLock(existingThrift, thrift);
		} finally {
			releasePrimaryKeyLock(thrift.getClass(), id);
		}
		return true;
	}

	/**
	 * Delete a thrift object. Delete operations in ZombieDB are atomic.
	 * 
	 * @param thrift
	 *            the object to delete
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void delete(
			T thrift) throws IOException;

	/**
	 * Delete a thrift object given its id. Delete operations in ZombieDB are
	 * atomic.
	 * 
	 * @param in
	 *            the type of object to delete
	 * @param id
	 *            the id of the object
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public <F extends TFieldIdEnum, T extends TBase<?, F>> void deleteFromId(
			Class<T> in, String id) throws IOException {
		T t = get(in, id);
		delete(t);
	}

	/**
	 * Destroy the underlying database and clean up any resources it holds.
	 * 
	 * @throws IOException
	 */
	public synchronized void destroy() throws IOException {
		databaseEngine.destroy();
	}

	/**
	 * Exports a CSV containing the top-level items in the object.
	 * 
	 * @param in
	 *            the type to dump
	 * @param basePath
	 *            a directory where to put the file. The file name will be in
	 *            the form (basePath)/(class name).csv
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void exportCsv(
			Class<T> in, File basePath) throws IOException {
		basePath.mkdirs();
		File file = new File(basePath, in.getSimpleName() + ".csv.bz2");
		T dummy;
		try {
			dummy = in.newInstance();
		} catch (Exception e1) {
			throw new IOException(e1);
		}
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(
				new BZip2CompressorOutputStream(new FileOutputStream(file))));
		TreeSet<Integer> validTags = new TreeSet<Integer>();
		List<String> tagNames = new ArrayList<String>();
		for (int a = 0; a < 1000000; a++) {
			if (dummy.fieldForId(a) != null) {
				validTags.add(a);
				tagNames.add(dummy.fieldForId(a).getFieldName());
			}
		}
		writer.writeNext(tagNames.toArray(new String[0]));

		try {
			ThriftWrapperIterator<F, T> it = getValueIterator(in);
			List<String> line = new ArrayList<String>();
			while (it.hasNext()) {
				try {
					T t = it.next();
					line.clear();

					for (int tag : validTags) {
						Object value = t.getFieldValue(t.fieldForId(tag));
						if (value == null) {
							line.add("");
						} else if (ClassUtils.isPrimitiveOrWrapper(value.getClass())
								|| value instanceof String) {
							line.add(value.toString());
						} else {
							line.add("");
						}
					}
					writer.writeNext(line.toArray(new String[0]));
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
		} finally {
			writer.close();
		}
	}

	/**
	 * Dump All objects of a given type to a file.
	 * 
	 * @param in
	 *            the type to dump
	 * @param basePath
	 *            a directory where to put the file. The file name will be in
	 *            the form (basePath)/(class name).sf
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void dump(
			Class<T> in, File basePath) throws IOException {
		basePath.mkdirs();
		File file = new File(basePath, in.getSimpleName() + ".zombiedb.bz2");
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				new BZip2CompressorOutputStream(new FileOutputStream(file))));
		try {
			ThriftWrapperIterator<F, T> it = getValueIterator(in);
			while (it.hasNext()) {
				try {
					T t = it.next();

					writer.write(jsonSerializer.toString(t, "ISO-8859-1"));
					writer.write("\n");
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
		} finally {
			writer.close();
		}
	}

	private String generateString(int length) {
		char[] text = new char[length];
		for (int i = 0; i < length; i++) {
			text[i] = CHARACTERS.charAt(random.nextInt(CHARACTERS.length()));
		}
		return new String(text);
	}

	/**
	 * Get a thrift object from the primary key (the id).
	 * 
	 * @param in
	 *            the type to get
	 * @param id
	 *            the id
	 * @return a thrift object or null if it was not found
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> T get(
			Class<T> in, String id) throws IOException;

	/**
	 * Gets the all keys for a particular type.
	 * 
	 * @param in
	 *            the type to get
	 * @return the keys
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> Set<String> getAllKeys(
			Class<T> in) throws IOException;

	/**
	 * Gets the all rows for a particular type as a map of id -> object.
	 * 
	 * @param in
	 *            the type to get
	 * @return a map of id -> object
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> Map<String, T> getAllRows(
			Class<T> in) throws IOException {
		Set<String> keys = getAllKeys(in);
		Map<String, T> out = new HashMap<String, T>();
		for (String key : keys) {
			T value = get(in, key);
			if (value != null) {
				out.put(key, value);
			}
		}
		return out;
	}

	/**
	 * Gets a list of objects from their IDs.
	 * 
	 * @param in
	 *            the type to get
	 * @param ids
	 *            the ids
	 * @return the list of objects found. If an object for an id is not found,
	 *         nothing is added to the list for that id.
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> List<T> getList(
			Class<T> in, Collection<String> ids) throws IOException {
		List<T> thrifts = new ArrayList<T>();

		for (String id : ids) {
			T thrift = get(in, id);
			if (thrift != null) {
				thrifts.add(thrift);
			}
		}
		return thrifts;
	}

	/**
	 * Gets an iterator over thrift objects for a particular type.
	 * 
	 * @param in
	 *            the in
	 * @return the value iterator
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> ThriftWrapperIterator<F, T> getValueIterator(
			Class<T> in) throws IOException;

	/**
	 * Load thrift objects from a dump file.
	 * 
	 * @param in
	 *            the type to get
	 * @param basePath
	 *            the directory containing the dump file
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void load(
			Class<T> in, File basePath) throws IOException {
		File file = new File(basePath, in.getSimpleName() + ".zombiedb.bz2");
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new BZip2CompressorInputStream(new FileInputStream(file))));

		try {
			int count = 0;
			while (true) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				try {
					T t = in.newInstance();
					jsonDeserializer.deserialize(t, line, "ISO-8859-1");
					upsert(t);
					if (count % 100 == 0)
						databaseEngine.commit();
					count++;
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
			databaseEngine.commit();
		} finally {
			reader.close();
		}
	}

	/**
	 * HBase invalidates RowLock objects when a table is disabled. Registering
	 * classes during initialization ensures that the table structure does not
	 * need to change at runtime, preventing disables while other threads may
	 * have a lock active.
	 * 
	 * @param thrift
	 *            The object to register.
	 * @throws IOException
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void register(
			Class<T> thriftClass) throws IOException;

	/**
	 * Get the number of objects of a particular type.
	 * 
	 * @param in
	 *            the type to get
	 * @return the number of objects.
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> int size(
			Class<T> in) throws IOException;

	/**
	 * Updates/Inserts a thrift object that has changed in memory. Upserting is
	 * atomic.
	 * 
	 * @param thrift
	 *            the thrift object to upserts
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void upsert(
			T thrift) throws IOException;

	protected abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void upsertNoPrimaryLock(
			T staleThrift, T thrift) throws IOException;

	/**
	 * Updates a thrift object that has changed in memory. The upsert is not
	 * always atomic which offers better performance but is not acceptable in
	 * all scenarios.
	 * 
	 * @param thrift
	 *            the thrift
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void upsertNonAtomic(
			T thrift) throws IOException;

	/**
	 * Wipe database.
	 * 
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void wipeDatabase() throws IOException {
		databaseEngine.wipeDatabase();
	}
}
