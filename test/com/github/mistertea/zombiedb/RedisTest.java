package com.github.mistertea.zombiedb;

import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;

import com.github.mistertea.zombiedb.engine.RedisDatabaseEngine;

//Ignore this test unless you have a redis server running
@Ignore
public class RedisTest extends DatabaseTestBase {
	@Before public void setUp() throws IOException {
		db = new RedisDatabaseEngine(0,true);
		dbm = new DatabaseEngineManager(db);
		idbm = new IndexedDatabaseEngineManager(db);
		concurrentConnections.add(new DatabaseConnection(db,dbm,idbm));
		for(int a=0;a<1;a++) {
			RedisDatabaseEngine dbLocal = new RedisDatabaseEngine(0,false);
			DatabaseEngineManager dbmLocal = new DatabaseEngineManager(dbLocal);
			IndexedDatabaseEngineManager idbmLocal = new IndexedDatabaseEngineManager(dbLocal);
			concurrentConnections.add(new DatabaseConnection(dbLocal,dbmLocal,idbmLocal));
		}
	}
}
