package com.myapp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class App 
{
    public static void main( String[] args )
    {
		try{
			Configuration conf = new Configuration();
			Path dfs = new Path("hdfs://localhost:9000/user/hadoop/input/test1.txt");
			FileSystem fs = dfs.getFileSystem(conf);
			String filename = "hdfs://localhost:9000/user/hadoop/input/test1.txt";
			FSDataOutputStream os = fs.create(dfs);
			byte[] buff = "hello world".getBytes();
			os.write(buff, 0, buff.length);
			System.out.println("Create: " + filename);
			//System.out.println( "Hello World!" );
			System.exit(0);
		}
		catch (Exception e){
			e.printStackTrace();
		}
    }
}
