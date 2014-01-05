package com.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        boolean first = true;
        StringBuilder fileNames = new StringBuilder();
        while (values.hasNext()) {
            if (!first)
                fileNames.append(", ");
            first = false;
            fileNames.append(values.next().toString());
        }
        output.collect(key, new Text(fileNames.toString()));
    }
}