package com.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    //private final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text location = new Text();

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        location.set(fileName);
        StringTokenizer itr = new StringTokenizer(line.toLowerCase());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            output.collect(word, location);
        }

    }
}