/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jhu.graph;

/**
 * Created by Guan Yue Wang (gwang39)
 * 2018/10/29
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class GraphDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");

        System.out.printf("Enron Driver: %s %s\n", inputPath, outputPath);
        Job job = Job.getInstance(conf, "Enron Processor");
        job.setJarByClass(getClass());

        job.setMapperClass(GraphMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MultipleWritable.class);
        job.setReducerClass(GraphReducer.class);
        job.setCombinerClass(GraphReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
