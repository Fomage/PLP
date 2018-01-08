package question2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;


public class ParseReducer extends Reducer<Text, Text, Text, Text> {

    private IntWritable totalWordCount = new IntWritable();

    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {

        StringBuilder newValue = new StringBuilder();
        newValue.append("1.0");
        for(Text v : values) {
        	if(v.toString().startsWith("#")) {
        		continue;
        	}
        	newValue.append(" ");
        	newValue.append(v.toString());
        }
        context.write(key, new Text(newValue.toString()));
    }
}
