package com.github.mistertea.zombiedb;

import java.io.IOException;

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
		concurrentConnections.add(new DatabaseConnection(db,dbm,idbm));
		for(int a=0;a<7;a++) {
			HBaseDatabaseEngine dbLocal = new HBaseDatabaseEngine("Test", false);
			DatabaseEngineManager dbmLocal = new DatabaseEngineManager(dbLocal);
			IndexedDatabaseEngineManager idbmLocal = new IndexedDatabaseEngineManager(dbLocal);
			concurrentConnections.add(new DatabaseConnection(dbLocal,dbmLocal,idbmLocal));
		}
	}
}
