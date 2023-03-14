package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

public class WriteFile {
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
                    String filePath = "/hdfstest/writefile";

                    FSDataOutputStream create = hdfs.create(new Path(filePath));

                    System.out.println("Step 1 Finish!");

                    String sayHi = "hello world hello data!";
                    byte[] buff = sayHi.getBytes();
                    create.write(buff, 0, buff.length);
                    create.close();
                    System.out.println("Step 2 Finish!");
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}