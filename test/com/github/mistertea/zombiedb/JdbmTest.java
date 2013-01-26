package com.github.mistertea.zombiedb;

import java.io.IOException;

import org.junit.Before;

import com.github.mistertea.zombiedb.engine.JdbmDatabaseEngine;

public class JdbmTest extends DatabaseTestBase {
	@Before public void setUp() throws IOException {
		db = new JdbmDatabaseEngine("", "TestDb", true, false, true, false);
		dbm = new DatabaseEngineManager(db);
		idbm = new IndexedDatabaseEngineManager(db);
		concurrentConnections.add(new DatabaseConnection(db,dbm,idbm));
		for(int a=0;a<7;a++) {
			concurrentConnections.add(new DatabaseConnection(db,
					new DatabaseEngineManager(db),
					new IndexedDatabaseEngineManager(db)));
		}
	}
}
