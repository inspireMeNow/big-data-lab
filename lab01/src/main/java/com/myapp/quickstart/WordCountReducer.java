package com.myapp.quickstart;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text arg0, Iterable<LongWritable> arg1,
                          Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        //super.reduce(arg0, arg1, arg2);
        int sum=0;
        for(LongWritable num:arg1)
        {
            sum += num.get();

        }
        context.write(arg0,new LongWritable(sum));


    }
}