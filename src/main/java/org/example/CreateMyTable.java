package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.*;
public class CreateMyTable {
    public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        String tableName = "mytb";
        String columnFamily = "mycf";
        create(tableName, columnFamily);
    }

    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "localhost");
        return conf;
    }
    public static void create(String tableName, String columnFamily)
            throws MasterNotRunningException, ZooKeeperConnectionException,
            IOException {
        Connection connection=ConnectionFactory.createConnection(getConfiguration());
        Admin hBaseAdmin=connection.getAdmin();
        if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {
            System.err.println("Table exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            hBaseAdmin.createTable(tableDesc);
            System.err.println("Create Table SUCCESS!");
        }
    }
}
