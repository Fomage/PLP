package question1;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;


public class FreqReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {

        int numberOfDocs = context.getConfiguration().getInt("numberOfDocuments", 0);
        int numberOfDocsWithKey = 0;
        Map<String,String> docFreq = new HashMap<>();
        for(Text val : values) {
        	String[] docnN = val.toString().split("=");
        	numberOfDocsWithKey++;
        	docFreq.put(docnN[0],docnN[1]);
        }
        for(String doc : docFreq.keySet()) {
        	String[] nN = docFreq.get(doc).split("/");
        	double tf = Double.parseDouble(nN[0])/Double.parseDouble(nN[1]);
        	double idf = Double.valueOf(numberOfDocs)/Double.valueOf(numberOfDocsWithKey);
        	context.write(new Text(key.toString() + "@" + doc),
        			new Text(Double.toString(tf*Math.log(idf))));
        	//new Text("(" + nN[0]+"/"+nN[1] + "," + numberOfDocsWithKey+"/"+numberOfDocs + "," + (tf*idf) + ")"));
        }
    }
}
