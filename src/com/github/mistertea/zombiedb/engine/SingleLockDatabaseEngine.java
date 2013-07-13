package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class SingleLockDatabaseEngine implements DatabaseEngine {
	Set<String> locks = new HashSet<String>();

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
	public synchronized void releaseLock(String family, String key)
			throws IOException {
		locks.remove(family+":"+key);
	}
}
