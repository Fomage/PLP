package question2;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;


public class UpdateReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {
    	double d = context.getConfiguration().getDouble("damping", 0.85);
    	double pr = 1-d;
    	StringBuilder newValues = new StringBuilder();
    	for(Text v : values) {
    		if(v.toString().startsWith("#")) {
    			continue;
    		}
    		String[] parsed = v.toString().split(" ");
    		pr += d*Double.parseDouble(parsed[1])/Double.parseDouble(parsed[2]);
    		newValues.append(" ");
    		newValues.append(parsed[0]);
    	}
    	context.write(key, new Text(pr + newValues.toString()));
    }
}
