package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

public class LocateFile {
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
                    Path file = new Path("/hdfstest/sample_data");
                    FileStatus fileStatus = hdfs.getFileStatus(file);

                    BlockLocation[] location = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
                    for (BlockLocation block : location) {
                        String[] hosts = block.getHosts();
                        for (String host : hosts) {
                            System.out.println("block:" +block + " host:"+ host);
                        }
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}