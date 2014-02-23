package com.github.mistertea.zombiedb.engine;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mistertea.zombiedb.CloseableIterator;
import com.github.mistertea.zombiedb.EmptyCloseableIteratorWrapper;
import com.sleepycat.bind.ByteArrayBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BerkeleyDBDatabaseEngine extends SingleLockDatabaseEngine {
  static class DbInfo {
    Database database;
    StoredMap<String, byte[]> map;

    public DbInfo(Database database, StoredMap<String, byte[]> map) {
      this.database = database;
      this.map = map;
    }
  }

  final Logger logger = LoggerFactory.getLogger(AstyanaxDatabaseEngine.class);

  private Environment myEnv;

  // The databases that our application uses
  private Map<String, DbInfo> classDbMaps = new HashMap<String, DbInfo>();

  private DatabaseConfig myDbConfig;
  private String baseDbDirectory;
  private String dbName;
  private static final String CLASS_CATALOG = "java_class_catalog";
  private StoredClassCatalog javaCatalog;

  private boolean readOnly;

  public BerkeleyDBDatabaseEngine(String baseDbDirectory, String dbName,
      boolean wipe, boolean transactional, boolean inMemory, boolean noCache,
      boolean readOnly) throws IOException {
    super();
    logger.info("Creating BDB Engine");
    this.baseDbDirectory = baseDbDirectory;
    this.dbName = dbName;
    this.readOnly = readOnly;

    if (inMemory) {
      throw new IOException("In-memory BDB not supported");
    }

    EnvironmentConfig myEnvConfig = new EnvironmentConfig();
    if (noCache) {
      // Can't disable cache, but can make the smallest cache possible
      myEnvConfig.setCacheSize(96 * 1024);
    }
    myDbConfig = new DatabaseConfig();

    // If the environment is read-only, then
    // make the databases read-only too.
    myEnvConfig.setReadOnly(readOnly);
    myDbConfig.setReadOnly(readOnly);
    myDbConfig.setSortedDuplicates(false);

    // If the environment is opened for write, then we want to be
    // able to create the environment and databases if
    // they do not exist.
    myEnvConfig.setAllowCreate(!readOnly);
    myDbConfig.setAllowCreate(!readOnly);

    // Allow transactions if we are writing to the database
    myEnvConfig.setTransactional(transactional);
    myDbConfig.setTransactional(transactional);

    if (wipe) {
      wipeDatabase();
    }

    // Open the environment
    File dbRoot = new File(baseDbDirectory + File.separator + dbName
        + File.separator);
    dbRoot.mkdir();
    myEnv = new Environment(dbRoot, myEnvConfig);

    // Create the catalog
    Database catalogDb = myEnv.openDatabase(null, CLASS_CATALOG, myDbConfig);
    javaCatalog = new StoredClassCatalog(catalogDb);
  }

  @Override
  public synchronized void clear(String family) {
    getOrCreateDb(family).clear();
    myEnv.compress();
  }

  @Override
  public synchronized boolean commit() {
    return true;
  }

  @Override
  public synchronized boolean containsKey(String className, String s) {
    return getOrCreateDb(className).containsKey(s);
  }

  @Override
  public synchronized void deleteKey(String className, String s) {
    getOrCreateDb(className).remove(s);
  }

  @Override
  public synchronized void destroy() {
    logger.info("Destroying BDB database");
    for (DbInfo dbInfo : classDbMaps.values()) {
      dbInfo.database.close();
    }
    classDbMaps.clear();
    if (myEnv != null) {
      javaCatalog.close();
      myEnv.cleanLog();
      myEnv.close();
    }
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public synchronized Set<String> getAllIds(String family) {
    return new HashSet<String>(getOrCreateDb(family).keySet());
  }

  @Override
  public synchronized byte[] getBytes(String className, String s) {
    return getOrCreateDb(className).get(s);
  }

  private synchronized StoredMap<String, byte[]> getOrCreateDb(String className) {
    DbInfo info = classDbMaps.get(className);
    if (info == null) {
      Database db = myEnv.openDatabase(null, className, myDbConfig);
      StoredMap<String, byte[]> map = new StoredMap<String, byte[]>(db,
          new StringBinding(), new ByteArrayBinding(), !readOnly);
      classDbMaps.put(className, new DbInfo(db, map));
      return classDbMaps.get(className).map;
    }
    return info.map;
  }

  @Override
  public synchronized CloseableIterator<byte[]> getValueIterator(String family) {
    Map<String, byte[]> classDbMap = getOrCreateDb(family);

    return new EmptyCloseableIteratorWrapper<byte[]>(classDbMap.values()
        .iterator());
  }

  @Override
  public synchronized int numValues(String family) {
    return getOrCreateDb(family).size();
  }

  @Override
  public synchronized void putBytesBatch(String className, String key,
      byte[] value) {
    getOrCreateDb(className).put(key, value);
  }

  @Override
  public synchronized void putBytesAtomic(String className, String key,
      byte[] value) {
    getOrCreateDb(className).put(key, value);
  }

  @Override
  public synchronized void wipeDatabase() throws IOException {
    for (DbInfo dbInfo : classDbMaps.values()) {
      dbInfo.database.close();
    }
    classDbMaps.clear();
    File baseDir = new File(baseDbDirectory + "/" + dbName);
    if (baseDir.exists()) {
      File files[] = baseDir.listFiles();
      for (File file : files) {
        file.delete();
      }
    }
  }
}
