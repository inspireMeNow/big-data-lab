package org.example;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
public class GetData {
    public static void main(String[] args) throws IOException {
        String tableName = "buyer";
        get(tableName, "20386");
    }
    public static Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "localhost");
        return conf;
    }
    public static void get(String tableName, String rowkey) throws IOException {
        Connection connection= ConnectionFactory.createConnection(getConfiguration());
        Table table=connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        byte[] value1 = result.getValue("reg_date".getBytes(), "2010-05-05:reg_ip".getBytes());
        byte[] value2 = result.getValue("reg_date".getBytes(), "2010-05-05:buyer_status".getBytes());
        System.err.println("line1:SUCCESS");
        System.err.println("line2:"
                + new String(value1) + "\t"
                + new String(value2));
    }
}