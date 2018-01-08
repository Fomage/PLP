package question2;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
// Overriding of the map method
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		String[] kv = valE.toString().split("\t");
		String[] values = kv[1].split(" ");
		context.write(new DoubleWritable(-Double.parseDouble(values[0])), new Text(kv[0]));
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






