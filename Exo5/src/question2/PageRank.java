package question2;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;


public class PageRank extends Configured implements Tool {


    public int run(String[] args) throws Exception {

    	getConf().setDouble("damping", 0.85);
    	
        Job parse = Job.getInstance(getConf());
        parse.setJobName("pageRank1");
        parse.setJarByClass(PageRank.class);
        parse.setMapperClass(ParseMapper.class);
        parse.setReducerClass(ParseReducer.class);
        parse.setMapOutputKeyClass(Text.class);
        parse.setMapOutputValueClass(Text.class);
        parse.setOutputKeyClass(TextInputFormat.class);
        parse.setOutputValueClass(TextOutputFormat.class);

        Path inputFilePath = new Path("/home/cloudera/e5q2");
        Path outputFilePath = new Path("pageRank1");
        FileInputFormat.setInputDirRecursive(parse, true);
        FileInputFormat.addInputPath(parse, inputFilePath);
        FileOutputFormat.setOutputPath(parse, outputFilePath);
        FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        
        parse.waitForCompletion(true);
        
        int nbIteration = 6;
        for(int iteration=0; iteration<nbIteration; iteration++) {
        	Job update = Job.getInstance(getConf());
            update.setJobName("pageRank"+(2*iteration+1));
            update.setJarByClass(PageRank.class);
            update.setMapperClass(UpdateMapper.class);
            update.setReducerClass(UpdateReducer.class);
            update.setMapOutputKeyClass(Text.class);
            update.setMapOutputValueClass(Text.class);
            update.setOutputKeyClass(TextInputFormat.class);
            update.setOutputValueClass(TextOutputFormat.class);

            inputFilePath = new Path("pageRank1");
            outputFilePath = new Path("pageRank2");
            FileInputFormat.setInputDirRecursive(update, true);
            FileInputFormat.addInputPath(update, inputFilePath);
            FileOutputFormat.setOutputPath(update, outputFilePath);
            fs = FileSystem.newInstance(getConf());
            if (fs.exists(outputFilePath)) {
                fs.delete(outputFilePath, true);
            }
            
            update.waitForCompletion(true);
            
            Job reverse = Job.getInstance(getConf());
            reverse.setJobName("pageRank"+(2*iteration+2));
            reverse.setJarByClass(PageRank.class);
            reverse.setMapperClass(ReverseMapper.class);
            reverse.setReducerClass(ReverseReducer.class);
            reverse.setMapOutputKeyClass(Text.class);
            reverse.setMapOutputValueClass(Text.class);
            reverse.setOutputKeyClass(TextInputFormat.class);
            reverse.setOutputValueClass(TextOutputFormat.class);

            inputFilePath = new Path("pageRank2");
            outputFilePath = new Path("pageRank1");
            FileInputFormat.setInputDirRecursive(reverse, true);
            FileInputFormat.addInputPath(reverse, inputFilePath);
            FileOutputFormat.setOutputPath(reverse, outputFilePath);
            fs = FileSystem.newInstance(getConf());
            if (fs.exists(outputFilePath)) {
                fs.delete(outputFilePath, true);
            }
            
            reverse.waitForCompletion(true);

        }
        
        Job sort = Job.getInstance(getConf());
        sort.setJobName("pageRank"+(nbIteration+2));
        sort.setJarByClass(PageRank.class);
        sort.setMapperClass(SortMapper.class);
        sort.setMapOutputKeyClass(DoubleWritable.class);
        sort.setMapOutputValueClass(Text.class);
        sort.setOutputKeyClass(TextInputFormat.class);
        sort.setOutputValueClass(TextOutputFormat.class);

        inputFilePath = new Path("pageRank1");
        outputFilePath = new Path("pageRank");
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

        PageRank exempleDriver = new PageRank();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

