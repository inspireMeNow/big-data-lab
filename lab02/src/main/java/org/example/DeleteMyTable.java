package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
public class DeleteMyTable {
    public static void main(String[] args) throws IOException {
        String tableName = "mytb";
        delete(tableName);
    }

    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "localhost");
        return conf;
    }

    public static void delete(String tableName) throws IOException {
        Connection connection=ConnectionFactory.createConnection(getConfiguration());
        Admin hAdmin = connection.getAdmin();
        if(hAdmin.tableExists(TableName.valueOf(tableName))){
            try {
                hAdmin.disableTable(TableName.valueOf(tableName));
                hAdmin.deleteTable(TableName.valueOf(tableName));
                System.err.println("Delete table Success");
            } catch (IOException e) {
                System.err.println("Delete table Failed ");
            }
        }else{
            System.err.println("table not exists");
        }
    }
}