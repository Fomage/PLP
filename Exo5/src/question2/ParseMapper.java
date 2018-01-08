package question2;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		// ignore commentaries
		if(valE.toString().startsWith("#")) {
			return;
		}
		String[] words = valE.toString().split("\t");
		context.write(new Text(words[0]), new Text(words[1]));
		context.write(new Text(words[1]), new Text("#"));//keeping track of leaves
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






