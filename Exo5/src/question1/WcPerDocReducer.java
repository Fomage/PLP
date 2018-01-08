package question1;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;


public class WcPerDocReducer extends Reducer<Text, Text, Text, Text> {

    private Text newKey = new Text();
    private Text newValue = new Text();

    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {

        int sum = 0;
        Map<String, Integer> counter = new HashMap<>();
        for(Text val : values) {
        	String[] wn = val.toString().split("=");
        	counter.put(wn[0], Integer.valueOf(wn[1]));
        	sum += Integer.parseInt(wn[1]);
        }
        
        for(String word : counter.keySet()) {
        	newKey.set(word + "@" + key.toString());
        	newValue.set(counter.get(word) + "/" + sum);
        	context.write(newKey, newValue);
        }
    }
}
