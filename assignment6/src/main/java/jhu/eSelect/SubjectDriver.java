package jhu.eSelect;

import jhu.avro.EmailSimple;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

;


/**
 * Guan Yue Wang
 * 2018/11/15
 */
public class SubjectDriver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");
        Job job = Job.getInstance(conf, "Enron Processor");
        job.setJarByClass(getClass());

        String sType = conf.get("sType");
        String regex = conf.get("regex");

            System.out.printf("Select - subject: %s %s\n", inputPath, outputPath);
           job.setMapperClass(SubjectMapper.class);
           AvroJob.setOutputKeySchema(job, EmailSimple.SCHEMA$);
           job.setOutputFormatClass(AvroKeyOutputFormat.class);
           job.setOutputValueClass(NullWritable.class);
           job.setNumReduceTasks(0);


        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(AvroKeyInputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
