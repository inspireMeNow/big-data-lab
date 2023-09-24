package com.myapp.quickstart;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        //super.map(key, value, context);
        //String[] words = StringUtils.split(value.toString());
        String[] words = StringUtils.split(value.toString(), " ");
        for(String word:words)
        {
            context.write(new Text(word), new LongWritable(1));

        }
    }
}
