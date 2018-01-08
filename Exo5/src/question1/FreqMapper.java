package question1;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class FreqMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text key = new Text();
	private Text word = new Text();
// Overriding of the map method
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		String line = valE.toString();
		String[] keyValue = line.split("\t");
		String[] wd = keyValue[0].split("@");
		key.set(wd[0]);
		word.set(wd[1] + "=" + keyValue[1]);
		context.write(key, word);// writing (word, document=n/N)
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






