package question1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;


public class TfIdf extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        Job wordCountPerDoc = Job.getInstance(getConf());
        wordCountPerDoc.setJobName("TfIdf1");
        wordCountPerDoc.setJarByClass(TfIdf.class);

        wordCountPerDoc.setMapperClass(WordCountMapper.class);
        wordCountPerDoc.setReducerClass(WordCountReducer.class);
        wordCountPerDoc.setMapOutputKeyClass(Text.class);

        wordCountPerDoc.setMapOutputValueClass(IntWritable.class);
        wordCountPerDoc.setOutputKeyClass(TextInputFormat.class);
        wordCountPerDoc.setOutputValueClass(TextOutputFormat.class);

        Path inputFilePath = new Path("/home/cloudera/e5q1");
        Path outputFilePath = new Path("tfidf1");
        FileInputFormat.setInputDirRecursive(wordCountPerDoc, true);
        FileInputFormat.addInputPath(wordCountPerDoc, inputFilePath);
        FileOutputFormat.setOutputPath(wordCountPerDoc, outputFilePath);
        FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        //counting documents
        FileStatus[] stat = fs.listStatus(inputFilePath);
        getConf().setInt("numberOfDocuments", stat.length);
        
        wordCountPerDoc.waitForCompletion(true);
        
        Job wcDocs = Job.getInstance(getConf());
        wcDocs.setJobName("TfIdf2");
        wcDocs.setJarByClass(TfIdf.class);

        wcDocs.setMapperClass(WcPerDocMapper.class);
        wcDocs.setReducerClass(WcPerDocReducer.class);
        wcDocs.setMapOutputKeyClass(Text.class);

        wcDocs.setMapOutputValueClass(Text.class);
        wcDocs.setOutputKeyClass(TextInputFormat.class);
        wcDocs.setOutputValueClass(TextOutputFormat.class);

        inputFilePath = new Path("tfidf1");
        outputFilePath = new Path("tfidf2");
        FileInputFormat.setInputDirRecursive(wcDocs, true);
        FileInputFormat.addInputPath(wcDocs, inputFilePath);
        FileOutputFormat.setOutputPath(wcDocs, outputFilePath);
        fs = FileSystem.newInstance(getConf());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        
        wcDocs.waitForCompletion(true);
        
        Job freq = Job.getInstance(getConf());
        freq.setJobName("TfIdf3");
        freq.setJarByClass(TfIdf.class);

        freq.setMapperClass(FreqMapper.class);
        freq.setReducerClass(FreqReducer.class);
        freq.setMapOutputKeyClass(Text.class);

        freq.setMapOutputValueClass(Text.class);
        freq.setOutputKeyClass(TextInputFormat.class);
        freq.setOutputValueClass(TextOutputFormat.class);

        inputFilePath = new Path("tfidf2");
        outputFilePath = new Path("tfidf3");
        FileInputFormat.setInputDirRecursive(freq, true);
        FileInputFormat.addInputPath(freq, inputFilePath);
        FileOutputFormat.setOutputPath(freq, outputFilePath);
        fs = FileSystem.newInstance(getConf());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        
        freq.waitForCompletion(true);
        
        Job sort = Job.getInstance(getConf());
        sort.setJobName("TfIdf4");
        sort.setJarByClass(TfIdf.class);

        sort.setMapperClass(SortMapper.class);
        //sort.setReducerClass(FreqReducer.class);
        sort.setMapOutputKeyClass(DoubleWritable.class);

        sort.setMapOutputValueClass(Text.class);
        sort.setOutputKeyClass(DoubleWritable.class);
        sort.setOutputValueClass(TextOutputFormat.class);

        inputFilePath = new Path("tfidf3");
        outputFilePath = new Path("tfidf");
        FileInputFormat.setInputDirRecursive(sort, true);
        FileInputFormat.addInputPath(sort, inputFilePath);
        FileOutputFormat.setOutputPath(sort, outputFilePath);
        fs = FileSystem.newInstance(getConf());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        
        sort.waitForCompletion(true);
        
        return  0;
    }


    public static void main(String[] args) throws Exception {

        TfIdf exempleDriver = new TfIdf();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

