package question3;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private final static DoubleWritable height = new DoubleWritable(1.);
	private Text word = new Text();
// Overriding of the map method
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
        // To complete according to the processing
        // Processing : keyI = ..., valI = ...
		String line = valE.toString();
		
		String[] tokens = line.split(";");
		word.set(tokens[3]);
		Double h = 0.;
		if(!tokens[6].isEmpty()) {
			try{
				h = Double.parseDouble(tokens[6]);
			} catch(NumberFormatException e) {
				// just ignore the data, leave it a 0
			}
		}
		height.set(h);
		context.write(word, height);
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






