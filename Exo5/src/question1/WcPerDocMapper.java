package question1;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class WcPerDocMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text key = new Text();
	private Text word = new Text();
// Overriding of the map method
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		String line = valE.toString();
		String[] wordAndDocCounter = line.split("\t");
		String[] wordAndDoc = wordAndDocCounter[0].split("@");
		key.set(wordAndDoc[1]);
		word.set(wordAndDoc[0] + "=" + wordAndDocCounter[1]);
		context.write(key, word);
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






