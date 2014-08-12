package mcalab.hbase.manager.rest;


import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;

public class TestTableManager {
	static String restURI = "YOUR_URI";
	static int restPort = 8080;
	static String masterURI = "YOUR_URI";
	static String zookeeperURI = "YOUR_URI";
	static String zookeeperPort = "2181";
	
	public static void main(String[] args) throws IOException {	
		test_PerformanceCheck();
	}
	
	public static void test_PerformanceCheck() throws IOException {
		long restAvg = 0;
		long start, end, temp;
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		int count = 100;
		
		final String tableName = "test1212";
		final String[] cols = new String[] {"col-1", "col-2"};
		final String newcols = "col-3";
		
		TableManager manager = new TableManager(restURI, restPort, masterURI, zookeeperURI, zookeeperPort);
		manager.deleteTable(tableName);
		
		for(int i = 0; i < count; i++) {
			start = System.currentTimeMillis();
			
			manager.createTable(tableName, cols);
			manager.createColumnFamily(tableName, newcols);
			manager.deleteColumnFamily(tableName, new String[] {"col-3"});
			manager.deleteTable("test1212");
			
			end = System.currentTimeMillis();
			temp = (end - start);
			min = (min > temp)? temp : min;
			max = (max < temp)? temp : max;
			restAvg += temp;
		}
		
		System.out.println("Min : " + min + "ms / Max : " + max + "ms. ");
		System.out.println("REST Table Schema Average : " + ((double)restAvg / count));
	}
}
