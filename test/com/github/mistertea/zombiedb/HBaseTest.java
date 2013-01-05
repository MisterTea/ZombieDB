package com.github.mistertea.zombiedb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.github.mistertea.zombiedb.engine.HBaseDatabaseEngine;

// Ignore this test unless you have an hbase server running
@Ignore
public class HBaseTest extends DatabaseTestBase {
	@Before public void setUp() throws IOException {
		db = new HBaseDatabaseEngine("Test", true);
		dbm = new DatabaseEngineManager(db);
		idbm = new IndexedDatabaseEngineManager(db);
	}
	
	@After public void shutDown() throws Exception {
		db.destroy();
	}
}
