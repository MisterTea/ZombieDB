package com.github.mistertea.zombiedb;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

import com.github.mistertea.zombiedb.engine.DatabaseEngine;

/**
 * The Class DatabaseEngineManager supports primary keys without secondary keys.  See {@link IndexedDatabaseEngineManager}
 * if you want secondary keys.
 */
public class DatabaseEngineManager extends AbstractDatabaseEngineManager {
	private final static Logger logger = Logger.getLogger(DatabaseEngineManager.class.getName());
	
	/**
	 * Instantiates a new database engine manager.
	 *
	 * @param databaseEngine the database engine
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public DatabaseEngineManager(DatabaseEngine databaseEngine) throws IOException {
		super(databaseEngine);
	}
	
	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#delete(org.apache.thrift.TBase)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void delete(T thrift) throws IOException {
		String id;
		try {
			id = (String)thrift.getFieldValue(thrift.fieldForId(1));
		} catch (Exception e) {
			throw new IOException(e);
		}
		databaseEngine.deleteKey(thrift.getClass().getSimpleName(), id);
	}
	
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> Set<String> getAllKeys(Class<T> in) throws IOException {
		return databaseEngine.getAllIds(in.getSimpleName());
	}

	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#get(java.lang.Class, java.lang.String)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> T get(Class<T> in, String id) throws IOException {
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
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> ThriftWrapperIterator<F,T> getValueIterator(Class <T> in) throws IOException {
		return new ThriftWrapperIterator<F,T>(in, deserializer, databaseEngine.getValueIterator(in.getSimpleName()));
	}

	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#size(java.lang.Class)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> int size(Class<T> in) throws IOException {
		return databaseEngine.numValues(in.getSimpleName());
	}
	
	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#upsert(org.apache.thrift.TBase)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void upsert(T thrift) throws IOException {
		try {
			String id = (String)thrift.getFieldValue(thrift.fieldForId(1));
			
			databaseEngine.putBytesAtomic(thrift.getClass().getSimpleName(), id, serializer.serialize(thrift));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	protected synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void upsertNoPrimaryLock(T staleThrift, T thrift) throws IOException {
		upsert(thrift);
	}
	
	/**
	 * @see com.github.mistertea.zombiedb.AbstractDatabaseEngineManager#upsertNonAtomic(org.apache.thrift.TBase)
	 */
	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void upsertNonAtomic(T thrift) throws IOException {
		try {
			String id = (String)thrift.getFieldValue(thrift.fieldForId(1));
			
			databaseEngine.putBytesBatch(thrift.getClass().getSimpleName(), id, serializer.serialize(thrift));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> T getAndLockPrimaryKey(
			Class<T> in, String id) throws IOException {
		databaseEngine.acquireLock(in.getSimpleName(), id);
		return get(in, id);
	}

	@Override
	protected <F extends TFieldIdEnum, T extends TBase<?, F>> void releasePrimaryKeyLock(Class<T> in, String id) throws IOException {
		databaseEngine.releaseLock(in.getSimpleName(), id);
	}

	@Override
	public synchronized <F extends TFieldIdEnum, T extends TBase<?, F>> void register(
			Class<T> thriftClass) throws IOException {
		get(thriftClass, "null");
		logger.info("Registered " + thriftClass.getSimpleName());
	}
}
