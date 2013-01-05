package com.github.mistertea.zombiedb.engine;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;


public class JdbmDatabaseEngine extends DatabaseEngine {
	private final static Logger logger = Logger.getLogger(JdbmDatabaseEngine.class.getName());

	Map<String, ConcurrentMap<String,byte[]>> classDbMaps = new HashMap<String, ConcurrentMap<String,byte[]>>();
	private String dbDirectory;
	private String dbFileName;

	private boolean inMemory;
	public DB database;

	public JdbmDatabaseEngine(String baseDbDirectory, String dbName, boolean wipe, boolean transactional, boolean inMemory, boolean noCache) throws IOException {
		super();
		this.inMemory = inMemory;
		logger.info("CREATING NEW DATABASE ENGINE");
		/** create (or open existing) database using builder pattern*/
		dbDirectory = baseDbDirectory + "/" + dbName;
		dbFileName = dbDirectory + "/" + "db";
		System.out.println(" db dir: " + dbDirectory + " db file" + dbFileName);
		if (wipe) {
			wipeDatabase();
		}
		new File(dbDirectory).mkdirs();
		DBMaker dbMaker = null;
		if(inMemory) {
			dbMaker = DBMaker.openMemory().disableCache();
		} else {
			dbMaker = DBMaker.openFile(dbFileName);
			if(noCache) {
				dbMaker.disableCache();
			} else {
				dbMaker.enableSoftCache();
			}
		}
		dbMaker.closeOnExit();
		if(!transactional) {
			dbMaker.disableTransactions();
		}
		database = dbMaker.make();
	}

	@Override
	public synchronized void clear(String family) {
		getOrCreateDb(family).clear();
	}

	@Override
	public synchronized void commit() {
		database.commit();
	}
	
	@Override
	public synchronized boolean containsKey(String className, String s) {
		return getOrCreateDb(className).containsKey(s);
	}

	@Override
	public synchronized boolean deleteKey(String className, String s) {
		return getOrCreateDb(className).remove(s)!=null;
	}

	@Override
	public synchronized void destroy() {
		if(database != null && !database.isClosed())
			database.close();
		database = null;
	}

	@Override
	public synchronized Set<String> getAllIds(String family) {
		return getOrCreateDb(family).keySet();
	}

	@Override
	public synchronized byte[] getBytes(String className, String s) {
		return getOrCreateDb(className).get(s);
	}

	public synchronized ConcurrentMap<String, byte[]> getOrCreateDb(String className) {
		ConcurrentMap<String, byte[]> dbMap = classDbMaps.get(className);
		if(dbMap == null) {
			dbMap = database.getHashMap(className);
			if (dbMap == null) {
				dbMap = database.createHashMap(className);
			}
		}
		classDbMaps.put(className, dbMap);
		return dbMap;
	}

	@Override
	public synchronized Iterator<byte[]> getValueIterator(String family) {
		Map<String,byte[]> classDbMap = getOrCreateDb(family);
		
		return classDbMap.values().iterator();
	}

	@Override
	public synchronized int numValues(String family) {
		return getOrCreateDb(family).size();
	}

	@Override
	public synchronized void putBytes(String className, String key, byte[] value) {
		getOrCreateDb(className).put(key, value);
	}

	@Override
	public synchronized void wipeDatabase() throws IOException {
		System.out.println("WIPING OLD DATABASE");
		if(database != null) {
			for(String s : database.getCollections().keySet()) {
				System.out.println("DELETING COLLECTION: " + s);
				database.deleteCollection(s);
				classDbMaps.remove(s);
			}
		}
		if(inMemory) {
			return;
		}
		// Wipe the old database
		File folder = new File(dbDirectory);
		File[] listOfFiles = folder.listFiles();
		if(listOfFiles == null) {
			if(!new File(dbDirectory).mkdir()) {
				throw new IOException("Could not make databse directory!");
			}
		} else {

			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					String dirFilename = listOfFiles[i].getPath();
					System.out.println(dirFilename);
					if(!new File(dirFilename).delete()) {
						throw new IOException("Could not delete file: " + dirFilename);
					}
					if(new File(dirFilename).delete()) {
						throw new IOException("Could not delete file: " + dirFilename);
					}
				}
			}
		}

		if (new File(dbFileName).delete()) {
			logger.info("WIPED OLD DATABASE");
		}
	}
}
