package jhu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by gwang39 on 10/14/2018
 * Guan Yue Wang
 */
public class WCCombiner extends Reducer<Text, IntWritable, Text, IntWritable> 
{
    int i = 0;
    IntWritable count = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for(IntWritable val : values) {
           i = i + 1;
        }
        count.set(i);
        context.write(key, count);
    }
}
