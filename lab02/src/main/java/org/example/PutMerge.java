package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

public class PutMerge {
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
                    FileSystem local = FileSystem.getLocal(conf);
                    String from_LinuxDir = "/home/dky/hadoop4/";
                    String to_HDFS = "/hdfstest/mergefile";
                    FileStatus[] inputFiles = local.listStatus(new Path(from_LinuxDir));
                    FSDataOutputStream out = hdfs.create(new Path(to_HDFS));

                    for (FileStatus file : inputFiles) {
                        FSDataInputStream in = local.open(file.getPath());
                        byte[] buffer = new byte[256];
                        int bytesRead = 0;
                        while ( (bytesRead = in.read(buffer) ) > 0) {
                            out.write(buffer, 0, bytesRead);
                        }
                        in.close();
                    }
                    System.out.println("Finish!");
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}