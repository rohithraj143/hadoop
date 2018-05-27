package com.hadoop.learn.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Rohith Raj 5/27/2018
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueStr = value.toString();
        String[] words = valueStr.split(" ");
        for (String word : words) {
            String s = word.toLowerCase();
            context.write(new Text(s), new IntWritable(1));
        }

    }
    
}
