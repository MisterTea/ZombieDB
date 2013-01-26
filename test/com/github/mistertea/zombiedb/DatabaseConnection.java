package com.github.mistertea.zombiedb;

import com.github.mistertea.zombiedb.engine.DatabaseEngine;

public class DatabaseConnection {
	public DatabaseEngine db;
	public DatabaseEngineManager dbm;
	public IndexedDatabaseEngineManager idbm;

	DatabaseConnection(DatabaseEngine db,
			DatabaseEngineManager dbm,
			IndexedDatabaseEngineManager idbm) {
		this.db = db;
		this.dbm = dbm;
		this.idbm = idbm;
	}
}
