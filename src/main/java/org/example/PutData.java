package org.example;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class PutData {
    public static void main(String[] args) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        String tableName = "buyer";
        String columnFamily = "reg_date";
        put(tableName, "20385", columnFamily, "2010-05-04:reg_ip", "124.64.242.30");
        put(tableName, "20385", columnFamily, "2010-05-04:buyer_status", "1");

        put(tableName, "20386", columnFamily, "2010-05-05:reg_ip", "117.136.0.172");
        put(tableName, "20386", columnFamily, "2010-05-05:buyer_status", "1");

        put(tableName, "20387", columnFamily, "2010-05-06:reg_ip", "114.94.44.230");
        put(tableName, "20387", columnFamily, "2010-05-06:buyer_status", "1");

    }
    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "localhost");
        return conf;
    }
    public static void put(String tableName, String row, String columnFamily,
                           String column, String data) throws IOException {
        Table table=ConnectionFactory.createConnection(getConfiguration()).getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(columnFamily),
                Bytes.toBytes(column),
                Bytes.toBytes(data));
        table.put(put);
        System.err.println("SUCCESS");
    }
}