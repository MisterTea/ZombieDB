package com.github.mistertea.zombiedb.engine;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.github.mistertea.zombiedb.CloseableIterator;
import com.github.mistertea.zombiedb.EmptyCloseableIteratorWrapper;

public class JdbmDatabaseEngine extends SingleLockDatabaseEngine {
	private final static Logger logger = Logger
			.getLogger(JdbmDatabaseEngine.class.getName());

	ConcurrentMap<String, ConcurrentMap<String, byte[]>> classDbMaps = new ConcurrentHashMap<String, ConcurrentMap<String, byte[]>>();
	private String dbDirectory;
	private String dbFileName;

	private boolean inMemory;
	private boolean transactional;
	private boolean noCache;

	public DB database;

	public JdbmDatabaseEngine(String baseDbDirectory, String dbName,
			boolean wipe, boolean transactional, boolean inMemory,
			boolean noCache) throws IOException {
		super();
		this.inMemory = inMemory;
		this.transactional = transactional;
		this.noCache = noCache;
		logger.info("CREATING NEW DATABASE ENGINE");
		/** create (or open existing) database using builder pattern */
		dbDirectory = baseDbDirectory + "/" + dbName;
		dbFileName = dbDirectory + "/" + "db";
		System.out.println(" db dir: " + dbDirectory + " db file" + dbFileName);
		if (wipe) {
			wipeDatabase();
			// Wipe calls createDatabase()
		} else {
			createDatabase();
		}
	}

	private void createDatabase() {
		new File(dbDirectory).mkdirs();
		DBMaker dbMaker = null;
		if (inMemory) {
			dbMaker = DBMaker.newDirectMemoryDB();
		} else {
			dbMaker = DBMaker.newFileDB(new File(dbFileName));
			if (noCache) {
				dbMaker.cacheDisable();
			} else {
				// dbMaker.cacheSoftRefEnable();
			}
		}
		dbMaker.closeOnJvmShutdown();
		if (!transactional) {
			dbMaker.syncOnCommitDisable().asyncWriteDisable()
					.writeAheadLogDisable();
		}
		database = dbMaker.make();
	}

	@Override
	public void clear(String family) {
		getOrCreateDb(family).clear();
	}

	@Override
	public boolean commit() {
		if (transactional) {
			database.commit();
		}
		return true;
	}

	@Override
	public boolean containsKey(String className, String s) {
		return getOrCreateDb(className).containsKey(s);
	}

	@Override
	public void deleteKey(String className, String s) {
		getOrCreateDb(className).remove(s);
	}

	@Override
	public synchronized void destroy() {
		if (database != null && !database.isClosed())
			database.close();
		database = null;
	}

	@Override
	public Set<String> getAllIds(String family) {
		return getOrCreateDb(family).keySet();
	}

	@Override
	public byte[] getBytes(String className, String s) {
		return getOrCreateDb(className).get(s);
	}

	public ConcurrentMap<String, byte[]> getOrCreateDb(String className) {
		ConcurrentMap<String, byte[]> dbMap = classDbMaps.get(className);
		if (dbMap != null) {
			return dbMap;
		}
		synchronized (this) {
			if (dbMap == null) {
				dbMap = database.getHashMap(className);
				if (dbMap == null) {
					dbMap = database.createHashMap(className, true, null, null);
				}
			}
			classDbMaps.put(className, dbMap);
			return dbMap;
		}
	}

	@Override
	public CloseableIterator<byte[]> getValueIterator(String family) {
		Map<String, byte[]> classDbMap = getOrCreateDb(family);

		return new EmptyCloseableIteratorWrapper<byte[]>(classDbMap.values().iterator());
	}

	@Override
	public int numValues(String family) {
		return getOrCreateDb(family).size();
	}

	@Override
	public void putBytesBatch(String className, String key, byte[] value) {
		getOrCreateDb(className).put(key, value);
	}

	@Override
	public void putBytesAtomic(String family, String key, byte[] value)
			throws IOException {
		putBytesBatch(family, key, value);
		commit();
	}

	@Override
	public synchronized void wipeDatabase() throws IOException {
		System.out.println("WIPING OLD DATABASE");
		if (database != null) {
			database.close();
			classDbMaps.clear();
		}
		if (!inMemory) {
			// Wipe the old database
			File folder = new File(dbDirectory);
			File[] listOfFiles = folder.listFiles();
			if (listOfFiles == null) {
				if (!new File(dbDirectory).mkdir()) {
					throw new IOException("Could not make databse directory!");
				}
			} else {

				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile()) {
						String dirFilename = listOfFiles[i].getPath();
						System.out.println(dirFilename);
						if (!new File(dirFilename).delete()) {
							throw new IOException("Could not delete file: "
									+ dirFilename);
						}
						if (new File(dirFilename).delete()) {
							throw new IOException("Could not delete file: "
									+ dirFilename);
						}
					}
				}
			}

			if (new File(dbFileName).delete()) {
				logger.info("WIPED OLD DATABASE");
			}
		}
		createDatabase();
	}
}
