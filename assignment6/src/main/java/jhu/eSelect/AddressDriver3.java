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
public class AddressDriver3 extends Configured implements Tool {

    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        String inputPath = conf.get("inputPath");
        String outputPath = conf.get("outputPath");
        Job job = Job.getInstance(conf, "Enron Processor");
        job.setJarByClass(getClass());

        String sType = conf.get("sType");
        String sOption1 = conf.get("sOption1");
        String regex1 = conf.get("regex1");
        String sOption2 = conf.get("sOption2");
        String regex2 = conf.get("regex2");
        String sOption3 = conf.get("sOption3");
        String regex3 = conf.get("regex3");
            
        
        System.out.printf("Select - address: %s %s\n", inputPath, outputPath);
        job.setMapperClass(AddressMapper3.class);
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
