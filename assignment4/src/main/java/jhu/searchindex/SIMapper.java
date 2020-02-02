package jhu.searchindex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by gwang39 on 10/14/2018.
 * Guan Yue Wang
 */
public class SIMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String inputPath = null;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        // read configuration information
        this.inputPath = context.getConfiguration().get("inputPath");
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        
        String cleanword="";
        String fName="";

        
        
        if(key.get() == 0) {
            // this is the first line of the file
            // Which files are we processing?
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            context.getCounter("FILE", filename).increment(1);
        }

        // find words
        String[] words = value.toString().split("\\s+");
        
        fName = ((FileSplit) context.getInputSplit()).getPath().getName();
        

        for(String w : words)
        {
            cleanword = w.toLowerCase().replaceAll("[^a-zA-Z ]","");

            if (!cleanword.equals(null) && !cleanword.equals(""))
            {
            cleanword = cleanword + "\t" + fName;
            context.write(new Text(cleanword), key);
            }
   
            
        }
    }

    
}
