package mcalab.hbase.manager.rest;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

public final class MetaTableManager extends TableManager {

	public MetaTableManager(String restServer, int restPort, String master, String zookeeper, String zkPort) {
		super(restServer, restPort, master, zookeeper, zkPort);
	}
	
	public HColumnDescriptor setOptionHColumnDescriptor(final HColumnDescriptor desc) {
		desc.setCompressionType(Algorithm.GZ);
		desc.setBloomFilterType(BloomType.ROW);
		desc.setBlockCacheEnabled(false);

		return desc;
	}
	
	/**
	 * it is due to move metatable handling class.
	 */
	public String getTableNameUsingAutoIncrement(final String tableName, final String incrementTable, final String type, final String columnFamily, final String qualifier) throws IOException {
		// auto increments of tailer number pasted on table name
		if (incrementTable == null)
			return tableName;
		
		long tailerNumber = incrementNumberInMetaTable(incrementTable, type, columnFamily, qualifier);
		StringBuilder sb = new StringBuilder(tableName);
		sb.append('-');
		sb.append(tailerNumber);
		
		return sb.toString();
	}

	/**
	 * <p>
	 * it is due to move metatable handling class.
	 * </p>
	 * 
	 * <p>
	 * private long <b>incrementNumberInMetaTable</b>(String metaTableName)
	 * throws IOException
	 * </p>
	 * 
	 * @param type : increment row in auto increment's meta table
	 * 
	 * @return if it run normal, return increment number value. but dosen't do
	 *         abnormal, return {@link java.lang.Long#MIN_VALUE Long.MIN_VALUE}
	 */
	private long incrementNumberInMetaTable(final String metaTableName, final String type, final String columnFamily, final String qualifier) throws IOException {
		Assert.assertNotNull(admin);
		long number = Long.MIN_VALUE;
		try (RemoteHTable table = new RemoteHTable(client, metaTableName)){
			boolean isAvailable = admin.isTableAvailable(metaTableName);						
			if (isAvailable)
				number = table.incrementColumnValue(Bytes.toBytes(type), Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), 1L);
		}

		return number;
	}
}
