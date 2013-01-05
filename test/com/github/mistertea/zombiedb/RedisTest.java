package com.github.mistertea.zombiedb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.github.mistertea.zombiedb.engine.RedisDatabaseEngine;

//Ignore this test unless you have a redis server running
@Ignore
public class RedisTest extends DatabaseTestBase {
	@Before public void setUp() throws IOException {
		db = new RedisDatabaseEngine(0);
		dbm = new DatabaseEngineManager(db);
		idbm = new IndexedDatabaseEngineManager(db);
	}
	
	@After public void shutDown() throws Exception {
		db.destroy();
	}
}
