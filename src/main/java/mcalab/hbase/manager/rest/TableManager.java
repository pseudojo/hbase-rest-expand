package mcalab.hbase.manager.rest;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.junit.Assert;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TableManager {
	protected String MASTER_URI = null;
	protected String ZOOKEEPER_URI = null;
	protected String ZOOKEEPER_PORT = null;
	
	protected final String HBASE_MASTER = "hbase.master";
	protected final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	protected final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
	
	protected Configuration config = null;
	
	protected Cluster cluster = null;
	protected Client client = null;
	protected RemoteAdmin admin = null;
	
	
	public TableManager(final String restServer, final int restPort, final String master, final String zookeeper, final String zkPort) {
		this.setAdmin(restServer, restPort, master, zookeeper, zkPort);
	}
	
	protected void setAdmin(final String restServer, final int restPort, final String master, final String zookeeper, final String zkPort) {
		setConfiguration(master, zookeeper, zkPort);
		
		cluster = new Cluster();
		cluster.add(restServer, restPort);
		
		client = new Client(cluster);
		
		admin = new RemoteAdmin(client, config);
		Assert.assertNotNull(admin);
	}
	
	protected void setConfiguration(final String master, final String zookeeper, final String zkPort) {
		Assert.assertNotNull((MASTER_URI = master));
		Assert.assertNotNull((ZOOKEEPER_URI = zookeeper));
		Assert.assertNotNull((ZOOKEEPER_PORT = zkPort));
		
		config = HBaseConfiguration.create();
		config.set(HBASE_MASTER, MASTER_URI);
		config.set(HBASE_ZOOKEEPER_QUORUM, ZOOKEEPER_URI);
		config.set(HBASE_ZOOKEEPER_CLIENT_PORT, ZOOKEEPER_PORT);
	}
	
	public void createTable(final String tableName, final String column) throws IOException {
		createTable(tableName, new String[] { column });
	}
	
	public void createTable(final String tableName, final String[] columns) throws IOException {
		ArrayList<HColumnDescriptor> descs = Lists.newArrayListWithExpectedSize(columns.length);
		for(String column : columns)
			descs.add(new HColumnDescriptor(column));
		
		createTable(tableName, descs.toArray(new HColumnDescriptor[descs.size()]));
	}
	
	public void createTable(final String tableName, final HColumnDescriptor desc) throws IOException {
		createTable(tableName, new HColumnDescriptor[] { desc });
	}
	
	/**
	 * Using HBase REST API, METHOD : PUT
	 * 
	 * @param tableName
	 * @param columns
	 * @throws IOException
	 */
	public void createTable(final String tableName, final HColumnDescriptor[] descs) throws IOException {
		Assert.assertNotNull(admin);

		if (admin.isTableAvailable(tableName)) {
			System.err.println("already exist table.");
			return;
		}
		
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		Assert.assertNotNull(descs);
		if (descs.length > 0) {
			for (HColumnDescriptor desc : descs) {
				tableDesc.addFamily(desc);
			}
		}

		admin.createTable(tableDesc);
	}
	
	/**
	 * Using HBase REST API, METHOD : DELETE
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void deleteTable(final String tableName) throws IOException {
		Assert.assertNotNull(admin);

		if (!admin.isTableAvailable(tableName)) {
			System.err.println("table[" + tableName + "] : don't exist");
			return;
		}
		
		admin.deleteTable(tableName);
	}
	
	public boolean isTableAvailable(final String tableName) throws IOException {
		Assert.assertNotNull(admin);
		
		return admin.isTableAvailable(tableName);
	}
	
	public void createColumnFamily(final String tableName, final String column) throws IOException {
		createColumnFamily(tableName, new String[] { column });
	}
	
	public void createColumnFamily(final String tableName, final String[] columns) throws IOException {
		ArrayList<HColumnDescriptor> descs = Lists.newArrayListWithExpectedSize(columns.length);
		for(String column : columns)
			descs.add(new HColumnDescriptor(column));
		
		createColumnFamily(tableName, descs.toArray(new HColumnDescriptor[descs.size()]));
	}
	
	public void createColumnFamily(final String tableName, final HColumnDescriptor desc) throws IOException {
		createColumnFamily(tableName, new HColumnDescriptor[] { desc });
	}
	
	/**
	 * using REST API, METHOD : POST<br><br>
	 * Reference<br>
	 * - org.apache.hadoop.hbase.rest.RemoteAdmin.createTable<br>
	 * - starbase.client.table.__init__.py (https://github.com/barseghyanartur/starbase/)
	 * 
	 * @param tableName
	 * @param desc
	 * @throws IOException
	 */
	public void createColumnFamily(final String tableName, final HColumnDescriptor[] descs) throws IOException {
		Assert.assertNotNull(admin);
		
		if (!admin.isTableAvailable(tableName)) {
			System.err.println("don't exist table.");
			return;
		}

		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		for(HColumnDescriptor desc : descs)
			tableDesc.addFamily(desc);
		
		// set table schema model
		TableSchemaModel model = new TableSchemaModel(tableDesc);
		
		int maxRetries = config.getInt("hbase.rest.client.max.retries", 10);
		long sleepTime = config.getLong("hbase.rest.client.sleep", 1000L);
		
		// transmit rest command
		StringBuilder path = new StringBuilder();
		path.append('/');
		
		// no access token
		
		path.append(tableName);
		path.append('/');
		path.append("schema");
		int code = 0;
		
		for (int i = 0; i < maxRetries; i++) {
			Response response = client.post(path.toString(),
					Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
			code = response.getCode();
			switch (code) {
			case 200:
				return;
			case 201:
				return;
			case 509:
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					throw (InterruptedIOException) new InterruptedIOException().initCause(e);
				}
				break;
			default:
				throw new IOException("create request to " + path.toString() + " returned " + code);
			}
		}
		throw new IOException("create request to " + path.toString() + " timed out");
	}
	
	public void deleteColumnFamily(final String tableName, final String column) throws IOException {
		deleteColumnFamily(tableName, new String[] { column });
	}
	
	public void deleteColumnFamily(final String tableName, final String[] columns) throws IOException {
		ArrayList<HColumnDescriptor> descs = Lists.newArrayListWithExpectedSize(columns.length);
		for(String column : columns) {
			descs.add(new HColumnDescriptor(column));
		}
		
		deleteColumnFamily(tableName, descs.toArray(new HColumnDescriptor[descs.size()])); 
	}
	
	public void deleteColumnFamiy(final String tableName, final HColumnDescriptor desc) throws IOException {
		deleteColumnFamily(tableName, new HColumnDescriptor[] { desc });
	}
	
	/**
	 * using HBase REST API, METHOD : PUT<br><br>
	 * Reference<br>
	 * - org.apache.hadoop.hbase.rest.RemoteAdmin.createTable<br>
	 * - starbase.client.table.__init__.py (https://github.com/barseghyanartur/starbase/)
	 * 
	 * @param tableName
	 * @param descs
	 * @throws IOException
	 */
	public void deleteColumnFamily(final String tableName, final HColumnDescriptor[] descs) throws IOException {
		Assert.assertNotNull(admin);
		
		if (!admin.isTableAvailable(tableName)) {
			System.err.println("don't exist table.");
			return;
		} else if(descs.length < 1) {
			System.err.println("don't exist column familys to delete.");
			return;
		}
		
		// Copy HTableDescriptor
		HTableDescriptor originHTD = getTableSchema(tableName).getTableDescriptor();
		HTableDescriptor changedHTD = copyHTableDescriptorExceptColumnDescriptor(originHTD);
		
		HashMap<String, HColumnDescriptor> deleteColumns = Maps.newHashMap();
		for(HColumnDescriptor column : descs) {
			deleteColumns.put(column.getNameAsString(), column);
		}
		
		// except delete column families
		Collection<HColumnDescriptor> originColumns = originHTD.getFamilies();
		for(HColumnDescriptor column : originColumns) {
			if(!deleteColumns.containsKey(column.getNameAsString()))
				changedHTD.addFamily(column);
		}
		
		TableSchemaModel model = new TableSchemaModel(changedHTD);
		
		int maxRetries = config.getInt("hbase.rest.client.max.retries", 10);
		long sleepTime = config.getLong("hbase.rest.client.sleep", 1000L);
		
		StringBuilder path = new StringBuilder();
		path.append('/');
		
		// no access token
		
		path.append(tableName);
		path.append('/');
		path.append("schema");
		int code = 0;
		
		for (int i = 0; i < maxRetries; i++) {
			Response response = client.put(path.toString(), Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
			code = response.getCode();
			switch (code) {
			case 201:
				return;
			case 509:
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					throw (InterruptedIOException) new InterruptedIOException().initCause(e);
				}
				break;
			default:
				throw new IOException("create request to " + path.toString() + " returned " + code);
			}
		}
		
		throw new IOException("create request to " + path.toString() + " timed out");
	}
	
	protected HTableDescriptor copyHTableDescriptorExceptColumnDescriptor(HTableDescriptor origin) {
		// Copy HTableDescriptor
		HTableDescriptor desc = new HTableDescriptor(origin.getName());
		for(Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : origin.getValues().entrySet()) {
			desc.setValue(e.getKey().get(), e.getValue().get());
		}
		for(Map.Entry<String, String> e : origin.getConfiguration().entrySet()) {
			desc.setConfiguration(e.getKey(), e.getValue());
		}
		
		return desc;
	}
	
	/**
	 * using HBase REST API, METHOD : GET<br>
	 * Reference : org.apache.hadoop.hbase.rest.RemoteAdmin.createTable<br>
	 * 
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public TableSchemaModel getTableSchema(final String tableName) throws IOException {
		StringBuilder path = new StringBuilder();
		path.append('/');
		path.append(tableName);
		path.append('/');
		path.append("schema");
		
		int maxRetries = config.getInt("hbase.rest.client.max.retries", 10);
		long sleepTime = config.getLong("hbase.rest.client.sleep", 1000L);
		
		int code = 0;
		for(int i = 0; i < maxRetries; i++) {
			Response response = client.get(path.toString(), Constants.MIMETYPE_PROTOBUF);
			code = response.getCode();
			
			switch(code) {
			case 200 :
				TableSchemaModel model = new TableSchemaModel();
				return (TableSchemaModel) model.getObjectFromMessage(response.getBody());
			case 404 :
				throw new IOException("Table doesn't exist.");
			case 509 :
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					throw (InterruptedIOException)new InterruptedIOException().initCause(e);
				}
				break;
			default : 
				throw new IOException("get request to " + path.toString() + " request returned " + code);
			}
		}
		throw new IOException("get request to " + path.toString() + " request timed out " + code);
	}

	@Override
	protected void finalize() throws Throwable {
		admin = null;
		config.clear();

		super.finalize();
	}
}
