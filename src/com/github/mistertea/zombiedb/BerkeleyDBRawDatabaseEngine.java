package com.github.mistertea.zombiedb;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BerkeleyDBRawDatabaseEngine extends DatabaseEngine {
    private Environment myEnv;

    // The databases that our application uses
    private Map<String, Database> classDbMaps = new HashMap<String, Database>();

	private DatabaseConfig myDbConfig;
	private String baseDbDirectory;
	private String dbName;
	
	public BerkeleyDBRawDatabaseEngine(String baseDbDirectory, String dbName, boolean wipe, boolean transactional, boolean inMemory, boolean noCache) throws IOException {
		super();
		this.baseDbDirectory = baseDbDirectory;
		this.dbName = dbName;
		
		if(inMemory) {
			throw new IOException("In-memory BDB not supported");
		}
		
		boolean readOnly = false;
		
        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        if(noCache) {
        	// Can't disable cache, but can make the smallest cache possible
        	myEnvConfig.setCacheSize(96 * 1024);
        }
        myDbConfig = new DatabaseConfig();

        // If the environment is read-only, then
        // make the databases read-only too.
        myEnvConfig.setReadOnly(readOnly);
        myDbConfig.setReadOnly(readOnly);

        // If the environment is opened for write, then we want to be
        // able to create the environment and databases if
        // they do not exist.
        myEnvConfig.setAllowCreate(!readOnly);
        myDbConfig.setAllowCreate(!readOnly);

        // Allow transactions if we are writing to the database
        myEnvConfig.setTransactional(transactional);
        myDbConfig.setTransactional(transactional);

        if(wipe) {
        	wipeDatabase();
        }
        
        // Open the environment
        File dbRoot = new File(baseDbDirectory + File.separator + dbName + File.separator);
        dbRoot.mkdir();
        myEnv = new Environment(dbRoot, myEnvConfig);
	}

	@Override
	public void wipeDatabase() throws IOException {
    	File baseDir = new File(baseDbDirectory + File.pathSeparator + dbName);
    	if(baseDir.exists()) {
    		File files[] = baseDir.listFiles();
    		for(File file : files) {
    			file.delete();
    		}
    	}
    	classDbMaps.clear();
	}

	private Database getOrCreateDb(String className) {
		Database dbMap = classDbMaps.get(className);
		if(dbMap == null) {
	        classDbMaps.put(className,myEnv.openDatabase(null,className,myDbConfig));
			return classDbMaps.get(className);
		}
		return dbMap;
	}
	
	@Override
	synchronized byte[] getBytes(String className, String s) {
		Database db = getOrCreateDb(className);

        DatabaseEntry key;
		try {
			key = new DatabaseEntry(s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
        DatabaseEntry value = new DatabaseEntry();
		db.get(null, key, value, null);
		return value.getData();
	}

	@Override
	synchronized void putBytes(String className, String keyString, byte[] valueBytes) {
		Database db = getOrCreateDb(className);

        DatabaseEntry key;
		try {
			key = new DatabaseEntry(keyString.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return;
		}
        DatabaseEntry value = new DatabaseEntry(valueBytes);
		db.put(null, key, value);
	}

	@Override
	synchronized boolean containsKey(String className, String s) {
		return getBytes(className,s)!=null;
	}

	@Override
	synchronized boolean deleteKey(String className, String keyString) throws IOException {
		Database db = getOrCreateDb(className);
        DatabaseEntry key;
		try {
			key = new DatabaseEntry(keyString.getBytes("UTF-8"));
			return db.delete(null, key) == OperationStatus.SUCCESS;
		} catch (UnsupportedEncodingException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected synchronized int numValues(String family) {
		Database db = getOrCreateDb(family);
		return (int)db.count();
	}

	@Override
	public synchronized Set<String> getAllIds(String family) {
		Database db = getOrCreateDb(family);
		Set<String> keys = new HashSet<String>();
        // Get a cursor
        Cursor cursor = db.openCursor(null, null);

        // DatabaseEntry objects used for reading records
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        try {
	        while (cursor.getNext(foundKey, foundData,
	                LockMode.DEFAULT) == OperationStatus.SUCCESS) {
	        	keys.add(new String(foundKey.getData(),"UTF-8"));
	        }
        } catch(UnsupportedEncodingException e) {
        	e.printStackTrace();
        }
        cursor.close();
        return keys;
	}

	@Override
	public synchronized Iterator<byte[]> getValueIterator(String family) {
		Database db = getOrCreateDb(family);
		List<byte[]> values = new ArrayList<byte[]>();
        // Get a cursor
        Cursor cursor = db.openCursor(null, null);

        // DatabaseEntry objects used for reading records
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        while (cursor.getNext(foundKey, foundData,
                LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        	values.add(foundData.getData());
        }
        cursor.close();
        
		return values.iterator();
	}

	@Override
	public synchronized void commit() {
	}

	@Override
	public synchronized void destroy() {
		if(myEnv != null) {
			myEnv.close();
		}
	}

	@Override
	public synchronized void clear(String family) {
		Database db = getOrCreateDb(family);
        // Get a cursor
		Transaction transaction = myEnv.beginTransaction(null, null);
        Cursor cursor = db.openCursor(transaction, null);

        // DatabaseEntry objects used for reading records
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        while (cursor.getNext(foundKey, foundData,
                LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        	cursor.delete();
        }
        cursor.close();
        
        transaction.commit();
	}
}
