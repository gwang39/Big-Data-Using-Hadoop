/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jhu.central;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


/**
 * Created by Guan Yue Wang (gwang39)
 * 2018/10/29
 */
public class CentralDriver extends Configured implements Tool {


public int run(String[] strings) throws Exception {
        
        // get configuration and input output path
        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");


        System.out.printf("degree-centrality: %s %s\n", inputPath, outputPath);
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(getClass());

        // set the Mapper, Combiner, and REducer Class
        job.setMapperClass(CentralMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MultipleWritable.class);
        job.setReducerClass(CentralReducer.class);
        job.setNumReduceTasks(1);

        // Set the Input Data Form
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
