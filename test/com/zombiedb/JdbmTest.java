package com.zombiedb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;

public class JdbmTest extends DatabaseTestBase {
	@Before public void setUp() throws IOException {
		db = new JdbmDatabaseEngine("", "TestDb", true, false, true, false);
		dbm = new DatabaseEngineManager(db);
		idbm = new IndexedDatabaseEngineManager(db);
	}
	
	@After public void shutDown() throws Exception {
		db.destroy();
	}
}
