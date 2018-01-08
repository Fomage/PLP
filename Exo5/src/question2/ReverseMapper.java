package question2;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class ReverseMapper extends Mapper<LongWritable, Text, Text, Text> {
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		String[] keyValue = valE.toString().split("\t");
		String[] values = keyValue[1].split(" ");
		for(int i=1; i<values.length; i++) {
			context.write(new Text(values[i]), new Text(keyValue[0]));
		}
		context.write(new Text(keyValue[0]), new Text("#"+values[0]));
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






