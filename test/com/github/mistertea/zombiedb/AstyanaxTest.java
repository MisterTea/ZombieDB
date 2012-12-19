package com.github.mistertea.zombiedb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.github.mistertea.zombiedb.AstyanaxDatabaseEngine;
import com.github.mistertea.zombiedb.DatabaseEngineManager;
import com.github.mistertea.zombiedb.IndexedDatabaseEngineManager;

// Ignore this test until EmbeddedCassandra works
@Ignore
public class AstyanaxTest extends DatabaseTestBase {
	@Before public void setUp() throws IOException {
		db = new AstyanaxDatabaseEngine("TestCluster", "TestDb", true);
		dbm = new DatabaseEngineManager(db);
		idbm = new IndexedDatabaseEngineManager(db);
	}
	
	@After public void shutDown() throws Exception {
		db.destroy();
	}
}
