package com.zombiedb;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

public class AstyanaxDatabaseEngine extends DatabaseEngine {
	private final static Logger logger = Logger.getLogger(AstyanaxDatabaseEngine.class.getName());
	private String dbName;
	private Cluster cluster;
	private Keyspace keySpace;
	private Map<String, ColumnFamily<String,String>> columnFamilyMaps = new HashMap<String, ColumnFamily<String,String>>();
	private AstyanaxContext<Cluster> context;
	private Map<FamilyRowPair, ColumnPrefixDistributedRowLock<String>> locks =
			new HashMap<FamilyRowPair, ColumnPrefixDistributedRowLock<String>>();

	public AstyanaxDatabaseEngine(String clusterName, String dbName, boolean wipe) throws IOException {
		super();
		logger.info("Creating Astyanax Engine");
		this.dbName = dbName;

		context = new AstyanaxContext.Builder()
		.forCluster(clusterName)
		.forKeyspace(dbName)
		.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
		.setDiscoveryType(NodeDiscoveryType.NONE)
		.setRetryPolicy(new BoundedExponentialBackoff(250, 5000, 100))
				)
				.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
				.setPort(9160)
				.setMaxConnsPerHost(1)
				.setSeeds("127.0.0.1:9160")
				.setSocketTimeout(10000000)
						)
						.withConnectionPoolMonitor(new Slf4jConnectionPoolMonitorImpl())
						.buildCluster(ThriftFamilyFactory.getInstance());

		context.start();
		cluster = context.getEntity();

		if(cluster.getKeyspace(dbName) == null) {
			logger.info(dbName + " not found");
			createDatabase();
			keySpace = cluster.getKeyspace(dbName);
		} else {
			logger.info(dbName + " found");
			keySpace = cluster.getKeyspace(dbName);
			if(wipe) {
				wipeDatabase();
			} else
				try {
					if(cluster.describeKeyspace(dbName) == null) {
						logger.info(dbName + " cannot be described");
						createDatabase();
					}
				} catch (ConnectionException e) {
					throw new IOException(e);
				}
		}
	}

	@Override
	public synchronized void wipeDatabase() throws IOException {
		try {
			if(cluster.describeKeyspace(dbName) != null) {
				cluster.dropKeyspace(dbName);
			}
		} catch (ConnectionException e) {
			throw new IOException(e);
		}
		createDatabase();
	}

	private void createDatabase() throws IOException {
		Map<String,String> optsMap = new HashMap<String,String>();
		optsMap.put("replication_factor","1");
		try {
			cluster.addKeyspace(cluster.makeKeyspaceDefinition().setName(dbName).setStrategyClass("SimpleStrategy").setStrategyOptions(optsMap));
		} catch (ConnectionException e) {
			throw new IOException(e);
		}
	}

	private ColumnFamily<String, String> getOrCreateColumnFamily(String columnFamilyName) throws IOException {
		ColumnFamily<String,String> retval = columnFamilyMaps.get(columnFamilyName);

		if(retval != null) {
			return retval;
		}

		ColumnFamilyDefinition cfDefinition;
		try {
			cfDefinition = keySpace.describeKeyspace().getColumnFamily(columnFamilyName);
		} catch (ConnectionException e1) {
			throw new IOException(e1);
		}
		if(cfDefinition == null) {
			try {
				cluster.addColumnFamily(cluster.makeColumnFamilyDefinition()
						.setKeyspace(dbName)
						.setName(columnFamilyName)
						.setComparatorType("UTF8Type")
						.setKeyValidationClass("UTF8Type")
						.addColumnDefinition(cluster.makeColumnDefinition().setName("Data").setValidationClass("BytesType"))
						);
			} catch (ConnectionException e) {
				throw new IOException(e);
			}
		}

		ColumnFamily<String, String> cf =
				new ColumnFamily<String, String>(
						columnFamilyName,              // Column Family Name
						StringSerializer.get(),   // Key Serializer
						StringSerializer.get());  // Column Serializer
		columnFamilyMaps.put(columnFamilyName, cf);
		return cf;
	}

	@Override
	protected synchronized byte[] getBytes(String className, String key) throws IOException {
		OperationResult<ColumnList<String>> result;
		try {
			result = keySpace.prepareQuery(getOrCreateColumnFamily(className))
			.getKey(key)
			.execute();
		} catch (ConnectionException e) {
			throw new IOException(e);
		}
		ColumnList<String> columns = result.getResult();

		if(columns.getColumnByName("Data")==null) {
			return null;
		}

		return columns.getColumnByName("Data").getByteArrayValue();
	}

	@Override
	protected synchronized void putBytes(String className, String key, byte[] value) throws IOException {
		try {
			keySpace.prepareColumnMutation(getOrCreateColumnFamily(className), key, "Data")
			.putValue(value, null)
			.execute();
		} catch (ConnectionException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected synchronized boolean containsKey(String className, String key) throws IOException {
		OperationResult<ColumnList<String>> result;
		try {
			result = keySpace.prepareQuery(getOrCreateColumnFamily(className))
			.getKey(key)
			.execute();
		} catch (ConnectionException e) {
			throw new IOException(e);
		}
		ColumnList<String> columns = result.getResult();

		return columns.getColumnByName("Data") != null;
	}

	@Override
	protected synchronized boolean deleteKey(String className, String key) throws IOException {
		if(!containsKey(className, key)) {
			return false;
		}
		
		MutationBatch m = keySpace.prepareMutationBatch();

		// Deleting an entire row
		m.withRow(getOrCreateColumnFamily(className), key).delete();

		try {
			m.execute();
		} catch (ConnectionException e) {
			throw new IOException(e);
		}
		return true;
	}

	@Override
	protected synchronized int numValues(String family) throws IOException {
		return getAllIds(family).size();
	}

	@Override
	public synchronized Iterator<byte[]> getValueIterator(String family) throws IOException {
		Rows<String, String> rows;
		try {
			rows = keySpace.prepareQuery(getOrCreateColumnFamily(family))
					.getAllRows()
					.setExceptionCallback(new ExceptionCallback() {
						@Override
						public boolean onException(ConnectionException e) {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e1) {
							}
							return true;
						}})
						.execute().getResult();
		} catch (ConnectionException e) {
			throw new IOException(e);
		}
		return new ThriftWrapperCassandraIterator(rows.iterator());
	}

	public class ThriftWrapperCassandraIterator implements Iterator<byte[]> {
		private Iterator<Row<String,String>> rowIterator;
		private byte[] nextObject;

		public ThriftWrapperCassandraIterator(Iterator<Row<String, String>> rowIterator) {
			this.rowIterator = rowIterator;
			nextObject = null;
		}

		/* We are going to hell for this, but hasNext needs to actually do a fetch to make sure there is a next
		 */
		@Override
		public boolean hasNext() {
			if(nextObject == null) {
				while(rowIterator.hasNext()) {
					nextObject = getNextObject();
					if(nextObject != null) {
						return true;
					}
				}
			}
			return false;
		}

		private byte[] getNextObject() {
			ColumnList<String> columns = rowIterator.next().getColumns();
			byte[] valueBytes = columns.getByteArrayValue("Data", null);
			return valueBytes;
		}

		@Override
		public byte[] next() {
			if(nextObject == null) {
				if(hasNext()==false) {
					throw new IllegalStateException("Tried to get next when no next was there");
				}
			}
			if(nextObject == null) {
				throw new IllegalStateException("Got next but it was null for some reason");
			}
			byte[] object = nextObject;
			nextObject = null;
			return object;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Unsupported Operation");
		}

	}

	@Override
	public synchronized void commit() {
	}

	@Override
	public synchronized void destroy() {
		context.shutdown();
	}

	@Override
	public synchronized void clear(String family) throws IOException {
		Set<String> keys = getAllIds(family);
		for(String key : keys) {
			deleteKey(family, key);
		}
	}

	@Override
	public Set<String> getAllIds(String family) throws IOException {
		Rows<String, String> result;
		try {
			result = keySpace.prepareQuery(getOrCreateColumnFamily(family))
			.getAllRows()
			.setRowLimit(10)
			.withColumnRange(new RangeBuilder().setLimit(10).build())
			.setExceptionCallback(new ExceptionCallback() {
				@Override
				public boolean onException(ConnectionException e) {
					try {
						e.printStackTrace();
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					}
					return true;
				}})
				.execute().getResult();
		} catch (ConnectionException e) {
			throw new IOException(e);
		}

		Set<String> retval = new HashSet<String>();
		for(Row<String,String> row : result) {
			if(row.getColumns().getColumnByName("Data") != null) {
				retval.add(row.getKey());
			}
		}

		return retval;
	}

	class FamilyRowPair {
		public String columnFamilyName;
		public String rowName;

		public FamilyRowPair(String columnFamilyName, String rowName) {
			this.columnFamilyName = columnFamilyName;
			this.rowName = rowName;
		}

		@Override
		public boolean equals(Object otherObject) {
			FamilyRowPair other = (FamilyRowPair)otherObject;
			return columnFamilyName.equals(other.columnFamilyName) &&
					rowName.equals(other.rowName);
		}
	}

	@Override
	public synchronized void acquireLock(String className, String key) throws IOException {
		ColumnPrefixDistributedRowLock<String> lock = 
				new ColumnPrefixDistributedRowLock<String>(keySpace, getOrCreateColumnFamily(className), key)
				.withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
				.expireLockAfter(10, TimeUnit.SECONDS);

		try {
			lock.acquire();
		}
		catch (Exception e) {
			throw new IOException(e);
		}

		locks.put(new FamilyRowPair(className, key), lock);
	}

	@Override
	public synchronized void releaseLock(String className, String key) throws IOException {
		ColumnPrefixDistributedRowLock<String> lock = locks.get(new FamilyRowPair(className, key));
		if(lock == null) {
			throw new IOException("Tried to release unknown lock");
		}
		
		try {
			lock.release();
		}
		catch (Exception e) {
			throw new IOException(e);
		}
	}
}
