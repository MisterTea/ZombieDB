package com.github.mistertea.zombiedb.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;


public class HBaseDatabaseEngine extends DatabaseEngine {
	class HBaseIteratorWrapper implements Iterator<byte[]> {
		private Iterator<Result> resultIterator;
		private byte[] family;

		public HBaseIteratorWrapper(byte[] family, Iterator<Result> resultIterator) {
			this.family = family;
			this.resultIterator = resultIterator;
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
		
	}
	private final static Logger logger = Logger.getLogger(HBaseDatabaseEngine.class.getName());
	private Configuration configuration;
	private HBaseAdmin admin;
	private String clusterName;
	private HTable table;
	private byte[] qualifier = "Data".getBytes("ISO-8859-1");
	private List<Row> commands = new ArrayList<Row>();

	private Set<String> knownFamilies = new HashSet<String>();

	public HBaseDatabaseEngine(String clusterName, boolean wipe) throws IOException {
		super();
		
		this.clusterName = clusterName;
		
		logger.info("Creating HBase Engine");
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
		commands.add(delete);
	}

	@Override
	public void commit() throws IOException {
		if(commands.isEmpty()) {
			return;
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
		
		if(!table.getTableDescriptor().hasFamily(family.getBytes("ISO-8859-1"))) {
			admin.disableTable(clusterName);
			admin.addColumn(clusterName, new HColumnDescriptor(family.getBytes("ISO-8859-1")));
			admin.enableTable(clusterName);
		}
		knownFamilies.add(family);
	}

	private void createTable() throws IOException {
	    table = new HTable(configuration, clusterName);
	    knownFamilies.clear();
	}

	@Override
	public boolean deleteKey(String family, String key) throws IOException {
		createColumnFamilyIfNecessary(family);
		Delete delete = new Delete(key.getBytes("ISO-8859-1"));
		delete.deleteColumn(family.getBytes("ISO-8859-1"), qualifier);
		table.delete(delete);
		return true;
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
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
	public Iterator<byte[]> getValueIterator(String family) throws IOException {
		createColumnFamilyIfNecessary(family);
		Scan scan = new Scan();
		byte[] familyBytes = family.getBytes("ISO-8859-1");
		scan.addFamily(familyBytes);
		ResultScanner scanner = table.getScanner(scan);
		return new HBaseIteratorWrapper(familyBytes, scanner.iterator());
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
	public void putBytes(String family, String key, byte[] value)
			throws IOException {
		createColumnFamilyIfNecessary(family);
		if(containsKey(family,key)) {
			// HBase is a multimap, but ZombieDB assumes a map.  This ensures HBase acts like a map.
			deleteKey(family,key);
		}
		Put put = new Put(key.getBytes("ISO-8859-1"));
		put.add(family.getBytes("ISO-8859-1"), qualifier, value);
		commands.add(put);
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

}
