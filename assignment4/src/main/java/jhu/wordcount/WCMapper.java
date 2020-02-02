package jhu.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by wilsopw1 on 2/25/17.
 * Modified by gwang39 on 10/14/2018
 * Guan Yue Wang
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String inputPath = null;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // read configuration information
        this.inputPath = context.getConfiguration().get("inputPath");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String cleanword="";
        boolean isStopword;
        
        //create a array of top 25 stopwords
        String[] stopwords = {"a","an","and","are","as","at","be","by","for","from",
                            "has","he","in","is","it","its","of","on","that","the",
                            "to","was","were","will","with"};
        
        if(key.get() == 0) {
            // this is the first line of the file
            // Which files are we processing?
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            context.getCounter("FILE", filename).increment(1);
        }

        // find words
        String[] words = value.toString().split("\\s+");
        
        for(String w : words)
        {
            cleanword = w.toLowerCase().replaceAll("[^a-zA-Z ]","");
            
            isStopword = Arrays.asList(stopwords).contains(cleanword);
            
            if (!cleanword.equals(null) && !cleanword.equals("") && !isStopword)
            {
                context.write(new Text(cleanword), new IntWritable(1));
            }
        }
    }

    
}
