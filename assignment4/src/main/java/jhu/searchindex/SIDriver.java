package jhu.searchindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


/**
 * Created by gwang39 on 10/14/2018.
 * Guan Yue Wang
 */
public class SIDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");

        System.out.printf("Search Index: %s %s\n", inputPath, outputPath);
        Job job = Job.getInstance(conf, "Search Index");
        job.setJarByClass(getClass());

        job.setMapperClass(SIMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(SIReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
