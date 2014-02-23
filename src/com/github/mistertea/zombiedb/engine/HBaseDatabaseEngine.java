package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mistertea.zombiedb.CloseableIterator;

public class HBaseDatabaseEngine implements DatabaseEngine {
	class HBaseIteratorWrapper implements CloseableIterator<byte[]> {
		private byte[] family;
    private ResultScanner scanner;
    private Iterator<Result> resultIterator;

		public HBaseIteratorWrapper(byte[] family, ResultScanner scanner) {
			this.family = family;
			this.scanner = scanner;
			this.resultIterator = scanner.iterator();
		}

		@Override
		public boolean hasNext() {
			return resultIterator.hasNext();
		}

		@Override
		public byte[] next() {
			return resultIterator.next().getValue(family, qualifier);
		}

		@Override
		public void remove() {
			resultIterator.remove();
		}

    @Override
    public void close() throws IOException {
      scanner.close();
    }
		
	}
	final Logger logger = LoggerFactory.getLogger(HBaseDatabaseEngine.class);
	private Configuration configuration;
	private HBaseAdmin admin;
	private String clusterName;
	private HTable table;
	private byte[] qualifier = "Data".getBytes("ISO-8859-1");
	private List<Row> commands = new ArrayList<Row>();

	private Set<String> knownFamilies = new HashSet<String>();
	private Map<String, HBaseLock> locks = new HashMap<String, HBaseLock>();

	public HBaseDatabaseEngine(String clusterName, boolean wipe) throws IOException {
		super();
		
		this.clusterName = clusterName;
		
		logger.debug("Creating HBase Engine");
		configuration = HBaseConfiguration.create();
	    configuration.clear();

	    configuration.set("hbase.zookeeper.quorum", "127.0.0.1");  // - Our two zookeeper machines
	    configuration.set("hbase.zookeeper.property.clientPort", "2181");  // - Port they are listening on.
	    
	    admin = new HBaseAdmin(configuration);
	    
	    if(wipe) {
	    	wipeDatabase();
	    }
		if(!admin.isTableEnabled(clusterName)) {
			admin.enableTable(clusterName);
		}
	    createTable();
	    
	    if(!admin.tableExists(clusterName)) {
		    admin.createTable(new HTableDescriptor(clusterName));
	    }
	}
	
	@Override
	public void clear(String family) throws IOException {
		createColumnFamilyIfNecessary(family);
		Delete delete = new Delete();
		delete.deleteFamily(family.getBytes("ISO-8859-1"));
		table.delete(delete);
	}

	@Override
	public boolean commit() throws IOException {
		if(commands.isEmpty()) {
			table.flushCommits();
			return true;
		}
		try {
			Object[] results = table.batch(commands);
			for(Object o : results) {
				if(o == null) {
					throw new IOException("Operation failed");
				}
			}
			table.flushCommits();
			commands.clear();
			return true;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean containsKey(String family, String key) throws IOException {
		createColumnFamilyIfNecessary(family);
		Get get = new Get(key.getBytes("ISO-8859-1"));
		return table.exists(get);
	}

	private void createColumnFamilyIfNecessary(String family) throws IOException {
		if(knownFamilies.contains(family)) {
			return;
		}
		
		// Wait for the table to become enabled
		for(int a=0;!admin.isTableEnabled(clusterName) && a<100;a++) {
			logger.debug("Waiting for table to become enabled...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				break;
			}
			continue;
		}
		
		if(!table.getTableDescriptor().hasFamily(family.getBytes("ISO-8859-1"))) {
			boolean done = false;
			logger.debug("ADDING FAMILY: " + family);
			while(true) {
				try {
					admin.disableTable(clusterName);
					if(admin.isTableEnabled(clusterName)) {
						continue;
					}
					break;
				} catch(TableNotEnabledException tnee) {
					if(table.getTableDescriptor().hasFamily(family.getBytes("ISO-8859-1"))) {
						// Someone else created this family for us
						done=true;
						break;
					}
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						break;
					}
				}
			}
			if(!done) {
				try {
					admin.addColumn(clusterName, new HColumnDescriptor(family.getBytes("ISO-8859-1")));
				} catch(InvalidFamilyOperationException ifoe) {
					// If another thread created the column, it's ok.
					if (!ifoe.getMessage().contains("already exists")) {
						throw new IOException(ifoe);
					}
				}
				admin.enableTable(clusterName);
			}
		}
		knownFamilies.add(family);
	}

	private void createTable() throws IOException {
	    table = new HTable(configuration, clusterName);
	    table.setAutoFlush(true);
	    knownFamilies.clear();
	}

	@Override
	public void deleteKey(String family, String key) throws IOException {
		//System.out.println("DELETING: " + family + " : " + key);
		createColumnFamilyIfNecessary(family);
		Delete delete = null;
		if(!locks.containsKey(key) || !locks.get(key).families.contains(family)) {
			delete = new Delete(key.getBytes("ISO-8859-1"));
		} else {
			delete = new Delete(key.getBytes("ISO-8859-1"), 0L, locks.get(key).lock);
		}
		delete.deleteColumns(family.getBytes("ISO-8859-1"), qualifier);
		table.delete(delete);
		logger.debug("DELETING: " + family + " : " + key);
	}

	@Override
	public void destroy() throws IOException {
		admin.close();
		table.close();
	}

	@Override
	public Set<String> getAllIds(String family) throws IOException {
		createColumnFamilyIfNecessary(family);
		Set<String> ids = new HashSet<String>();
		Scan scan = new Scan();
		scan.addFamily(family.getBytes("ISO-8859-1"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
			ids.add(new String(rs.getRow(), "ISO-8859-1"));
		}
		return ids;
	}

	@Override
	public byte[] getBytes(String family, String key) throws IOException {
		createColumnFamilyIfNecessary(family);
		Get get = new Get(key.getBytes("ISO-8859-1"));
		Result result = table.get(get);
		return result.getValue(family.getBytes("ISO-8859-1"), qualifier);
	}
	
	@Override
	public CloseableIterator<byte[]> getValueIterator(String family) throws IOException {
		createColumnFamilyIfNecessary(family);
		Scan scan = new Scan();
		byte[] familyBytes = family.getBytes("ISO-8859-1");
		scan.addFamily(familyBytes);
		ResultScanner scanner = table.getScanner(scan);
		return new HBaseIteratorWrapper(familyBytes, scanner);
	}

	@Override
	public int numValues(String family) throws IOException {
		createColumnFamilyIfNecessary(family);
		int number=0;
		Scan scan = new Scan();
		scan.addFamily(family.getBytes("ISO-8859-1"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
			number++;
		}
		return number;
	}

	@Override
	public void putBytesBatch(String family, String key, byte[] value)
			throws IOException {
		//System.out.println("PUT BYTES NON ATOMIC " + family + " : " + key);
		createColumnFamilyIfNecessary(family);
		HBaseLock lock = locks.get(key);
		if(lock == null) {
			throw new IOException("No lock found for " + key);
		}
		Put put = new Put(key.getBytes("ISO-8859-1"), lock.lock);
		put.add(family.getBytes("ISO-8859-1"), qualifier, value);
		commands.add(put);
	}

	@Override
	public void putBytesAtomic(String family, String key, byte[] value)
			throws IOException {
		//System.out.println("PUT BYTES " + family + " : " + key);
		createColumnFamilyIfNecessary(family);
		Put put = null;
		if (locks.containsKey(key)) {
			put = new Put(key.getBytes("ISO-8859-1"), locks.get(key).lock);
		} else {
			put = new Put(key.getBytes("ISO-8859-1"));
		}
		put.add(family.getBytes("ISO-8859-1"), qualifier, value);
		table.put(put);
	}
	
	@Override
	public synchronized void wipeDatabase() throws IOException {
		if (admin.tableExists(clusterName)) {
			if(admin.isTableEnabled(clusterName)) {
				admin.disableTable(clusterName);
			}
			admin.deleteTable(clusterName);
		}
	    admin.createTable(new HTableDescriptor(clusterName));
	    createTable();
	}
	
	class HBaseLock {
		RowLock lock;
		Set<String> families = new HashSet<String>();
		
		HBaseLock(String key) throws IOException {
			while(true) {
				try {
					lock = table.lockRow(key.getBytes("ISO-8859-1"));
					return;
				} catch (UnsupportedEncodingException e) {
					throw new IOException(e);
				} catch (RetriesExhaustedException ree) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						throw new IOException(e);
					}
					logger.warn("Retries exhausted while locking row " + key);
				} catch (IOException e) {
					throw new IOException(e);
				}
			}
		}
	}

	@Override
	public void acquireLock(String family, String key) throws IOException {
		createColumnFamilyIfNecessary(family);
		HBaseLock lock = locks.get(key);
		if(lock == null) {
			logger.debug(String.valueOf(Thread.currentThread().getId()) + " GETTING LOCK " + family + " : " + key);
			lock = new HBaseLock(key);
			logger.debug(String.valueOf(Thread.currentThread().getId()) + " GOT LOCK " + family + " : " + key);
		}
		lock.families.add(family);
		locks.put(key, lock);
	}

	@Override
	public void releaseLock(String family, String key) throws IOException {
		createColumnFamilyIfNecessary(family);
		HBaseLock lock = locks.get(key);
		if(lock == null) {
			// Assume the entry was deleted
			return;
		}
		lock.families.remove(family);
		if (lock.families.isEmpty()) {
			try {
				logger.debug(String.valueOf(Thread.currentThread().getId()) + " RELEASING LOCK " + family + " : " + key);
				table.unlockRow(lock.lock);
				logger.debug(String.valueOf(Thread.currentThread().getId()) + " RELEASED LOCK " + family + " : " + key);
			} catch(UnknownRowLockException urle) {
				// This is ok, it means we deleted the row and so the lock cannot be found.
				logger.info("Could not find row lock: " + key, urle);
			}
			locks.remove(key);
		}
	}

}
