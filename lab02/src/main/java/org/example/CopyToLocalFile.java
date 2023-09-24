package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

public class CopyToLocalFile {
    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
            UserGroupInformation ugi
                    = UserGroupInformation.createRemoteUser("dky");

            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://duan-dky.tpddns.cn:9000");
                    conf.set("hadoop.job.ugi", "dky");

                    FileSystem hdfs = FileSystem.get(conf);
                    String from_HDFS = "/hdfstest/sample_data";
                    String to_Linux = "/home/dky/copytolocal";

                    hdfs.copyToLocalFile(false, new Path(from_HDFS), new Path(to_Linux));

                    System.out.println("Finish!");
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}