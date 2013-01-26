package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class SingleLockDatabaseEngine implements DatabaseEngine {
	ConcurrentSkipListSet<String> locks = new ConcurrentSkipListSet<String>();

	@Override
	public void acquireLock(String family, String key) throws IOException {
		while(true) {
			synchronized (this) {
				if(!locks.contains(family+":"+key)) {
					locks.add(family+":"+key);
					return;
				}
			}
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void releaseLock(String family, String key) throws IOException {
		locks.remove(family+":"+key);
	}
}
