package jhu.wordcount;

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
 * Created by wilsopw1 on 2/25/17.
 * Modified by gwang39 on 10/14/2018.
 * Guan Yue Wang
 */
public class WCDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        
        // get configuration and input output path
        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");

        System.out.printf("Word Count: %s %s\n", inputPath, outputPath);
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(getClass());

        // set the Mapper, Combiner, and REducer Class
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(WCCombiner.class);
        job.setReducerClass(WCReducer.class);
        job.setNumReduceTasks(1);

        // Set the Input Data Form
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
