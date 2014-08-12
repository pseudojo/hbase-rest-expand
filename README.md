hbase-rest-expand
=================

The Expansion of HBase REST API in Java

Add function
  - Table
    - createTable(String tableName, HColumnDescriptor[] descriptors)
    - getTableSchema(String tableName)
  
  - ColumnFamily
    - createColumnFamily(String tableName, String[] columns)
    - createColumnFamily(String tableName, HColumnDescriptor[] descriptors)
    - deleteColumnFamily(String tableName, String[] columns)
    - deleteColumnFamily(String tableName, HColumnDescriptor[] descriptors)

Thanks for reading.

Made and Modified by pseudojo
