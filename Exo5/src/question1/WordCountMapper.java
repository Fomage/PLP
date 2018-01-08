package question1;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
// Overriding of the map method
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
        // To complete according to the processing
        // Processing : keyI = ..., valI = ...
		String line = valE.toString();
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens())
		{
			String w = tokenizer.nextToken().replaceAll(" |\\,|\\?|;|\\.|:|!|'|\"|\\(|\\) $", "");
			if(!w.isEmpty()) {
				word.set(w + "@" + fileName);
				context.write(word, one);
			}
		}
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






