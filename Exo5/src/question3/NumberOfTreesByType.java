package question3;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

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


public class NumberOfTreesByType extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job = Job.getInstance(getConf());

        job.setJobName("NumberOfTreesByType");


        // On précise les classes MyProgram, Map et Reduce

        job.setJarByClass(NumberOfTreesByType.class);

        job.setMapperClass(NumberOfTreesMapper.class);

        job.setReducerClass(NumberOfTreesReducer.class);


        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(TextInputFormat.class);

        job.setOutputValueClass(TextOutputFormat.class);

        Path inputFilePath = new Path("/home/cloudera/arbres.csv");
        Path outputFilePath = new Path("numberOfTrees");
        

     // On accepte une entree recursive
        FileInputFormat.setInputDirRecursive(job, true);

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        return job.waitForCompletion(true) ? 0: 1;
    }


    public static void main(String[] args) throws Exception {

        NumberOfTreesByType exempleDriver = new NumberOfTreesByType();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

