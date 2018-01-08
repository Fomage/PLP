package question2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;


public class ReverseReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {
    	StringBuilder newValues = new StringBuilder();
    	String pr = "";
    	for(Text val : values) {
    		String v = val.toString();
    		if(v.startsWith("#")) {
    			pr=v.substring(1);
    			continue;
    		}
    		newValues.append(" ");
    		newValues.append(v);
    	}
    	context.write(key, new Text(pr + newValues.toString()));
    }
}
