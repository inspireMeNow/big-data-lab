package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

public class IteratorListFiles {
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

                    String watchHDFS = "/";

                    iteratorListFile(hdfs, new Path(watchHDFS));
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void iteratorListFile(FileSystem hdfs, Path path)
            throws FileNotFoundException, IOException {
        FileStatus[] files = hdfs.listStatus(path);
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                System.out.println(file.getPermission() + " " + file.getOwner()
                        + " " + file.getGroup() + " " + file.getPath());
                iteratorListFile(hdfs, file.getPath());
            } else if (file.isFile()) {
                System.out.println(file.getPermission() + " " + file.getOwner()
                        + " " + file.getGroup() + " " + file.getPath());
            }
        }
    }
}
