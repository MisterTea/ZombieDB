package com.github.mistertea.zombiedb;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;

import com.github.mistertea.zombiedb.engine.DatabaseEngine;

/**
 * The Class AbstractDatabaseEngineManager is a base class for both database engines.
 */
public abstract class AbstractDatabaseEngineManager {
	
	protected TSerializer serializer;
	protected TDeserializer deserializer;
	protected Random random = new Random(System.currentTimeMillis());
	protected DatabaseEngine databaseEngine;
	private static final String CHARACTERS = "123456789qwertyuiopasdfghjklzxcvbnm";

	/**
	 * Instantiates a new abstract database engine manager.
	 *
	 * @param databaseEngine the database engine backing this manager
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public AbstractDatabaseEngineManager(DatabaseEngine databaseEngine) throws IOException {
		this.databaseEngine = databaseEngine;
		TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
		serializer = new TSerializer(protocolFactory);
		deserializer = new TDeserializer(protocolFactory);
	}
	
	/**
	 * Removes all objects of type T from the database.
	 *
	 * @param in the class to clear
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void clear(Class<T> in) throws IOException {
		databaseEngine.clear(in.getSimpleName());
	}
	
	/**
	 * Commits any pending changes to the database.  Note that some database engines will flush changes to the database
	 * even without a commit.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized void commit() throws IOException {
		databaseEngine.commit();
	}
	
	/**
	 * Creates a new entry in the database for a thrift object.  Note that this method expects
	 * the object to have no id.  For objects that already have a unique id, use {@link #createWithId(TBase) createWithId}.
	 *
	 * @param thrift the object to create
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("unchecked")
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void create(T thrift) throws IOException {
		String prevKey = (String)thrift.getFieldValue(thrift.fieldForId(1));
		if(prevKey != null) {
			throw new IOException("Tried to autogenerate a key for an object that already had a key");
		}
		
		String s = null;
		do {
			s = generateString(16);
		} while(get(thrift.getClass(), s) != null);
		
		try {
			thrift.setFieldValue(thrift.fieldForId(1), s);
			update(thrift);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Creates a new entry in the database for a thrift object.  Note that this method expects
	 * the object to have an id.  For objects that do not have a unique id populated,
	 * use {@link #create(TBase) create}.
	 *
	 * @param thrift the object to create
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("unchecked")
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void createWithId(T thrift) throws IOException {
		try {
			String s = (String)thrift.getFieldValue(thrift.fieldForId(1));
			if(get(thrift.getClass(), s) != null) {
				throw new IOException("Tried to create a new record with a forced duplicate ID: " + s);
			}

			update(thrift);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Delete a thrift object.
	 *
	 * @param thrift the object to delete
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void delete(T thrift) throws IOException;
	
	/**
	 * Delete a thrift object given its id.
	 *
	 * @param in the type of object to delete
	 * @param id the id of the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public <F extends TFieldIdEnum, T extends TBase<?, F>> void deleteFromId(Class<T> in, String id) throws IOException {
		T t = get(in,id);
		delete(t);
	}
	
	/**
	 * Destroy the underlying database and clean up any resources it holds.
	 */
	public synchronized void destroy() {
		databaseEngine.destroy();
	}

	/**
	 * Dump All objects of a given type to a file.
	 *
	 * @param in the type to dump
	 * @param basePath a directory where to put the file.  The file name will be in the form (basePath)/(class name).sf
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void dump(Class<T> in, String basePath) throws IOException {
		String filename = basePath + "/" + in.getSimpleName() + ".sf";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filename), conf);
		Path path = new Path(filename);
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
		
		ThriftWrapperIterator<F,T> it = getValueIterator(in);
		while(it.hasNext()) {
			try {
				T t = it.next();
				String id = (String)t.getFieldValue(t.fieldForId(1));
			
				writer.append(new Text(id), new BytesWritable(serializer.serialize(t)));
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		writer.close();
	}

	private String generateString(int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = CHARACTERS.charAt(random.nextInt(CHARACTERS.length()));
	    }
	    return new String(text);
	}

	/**
	 * Get a thrift object from the primary key (the id).
	 *
	 * @param in the type to get
	 * @param id the id
	 * @return a thrift object or null if it was not found
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> T get(Class<T> in, String id) throws IOException;

	/**
	 * Gets the all keys for a particular type.
	 *
	 * @param in the type to get
	 * @return the keys
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> Set<String> getAllKeys(Class<T> in) throws IOException {
		return databaseEngine.getAllIds(in.getSimpleName());
	}

	/**
	 * Gets the all rows for a particular type as a map of id -> object.
	 *
	 * @param in the type to get
	 * @return a map of id -> object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> Map<String, T> getAllRows(Class<T> in) throws IOException {
		Iterator<T> it = new ThriftWrapperIterator<F,T>(in, deserializer,
				databaseEngine.getValueIterator(in.getSimpleName()));
		Map<String, T> out = new HashMap<String, T>();
		while(it.hasNext()) {
			T t = it.next();
			String id = (String)t.getFieldValue(t.fieldForId(1));
			out.put(id, t);
		}
		return out;
	}

	/**
	 * Gets a list of objects from their IDs.
	 *
	 * @param in the type to get
	 * @param ids the ids
	 * @return the list of objects found.  If an object for an id is not found, nothing is added to the list for that id.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> List<T> getList(Class<T> in, Collection<String> ids) throws IOException {
		List<T> thrifts = new ArrayList<T>();
		
		for(String id : ids) {
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
	 * @param in the in
	 * @return the value iterator
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> ThriftWrapperIterator<F,T> getValueIterator(Class <T> in) throws IOException;

	/**
	 * Load thrift objects from a dump file.
	 *
	 * @param in the type to get
	 * @param basePath the directory containing the dump file
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void load(Class<T> in, String basePath) throws IOException {
		String filename = basePath + "/" + in.getSimpleName() + ".sf";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filename), conf);
		Path path = new Path(filename);
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		try {
			int count=0;
			while(reader.next(key, value)) {
				try {
					T t = in.newInstance();
					deserializer.deserialize(t, value.getBytes());
					update(t);
					if(count%100==0)
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
	 * Get the number of objects of a particular type.
	 *
	 * @param in the type to get
	 * @return the number of objects.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> int size(Class<T> in) throws IOException;

	/**
	 * Updates a thrift object that has changed in memory.
	 *
	 * @param thrift the thrift
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract <F extends TFieldIdEnum, T extends TBase<?, F>> void update(T thrift) throws IOException;

	/**
	 * Wipe database.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void wipeDatabase() throws IOException {
		databaseEngine.wipeDatabase();
	}
}
