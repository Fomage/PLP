package question3;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;


public class MaxHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable maxHeight = new DoubleWritable(0.);

    @Override
    public void reduce(final Text key, final Iterable<DoubleWritable> values,
            final Context context) throws IOException, InterruptedException {

        double max = 0.;
        Iterator<DoubleWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
        	double n = iterator.next().get();
            if(n > max) {
            	max = n;
            }
        }

        maxHeight.set(max);
        context.write(key, maxHeight);
    }
}
