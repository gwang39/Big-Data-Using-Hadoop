package jhu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wilsopw1 on 2/25/17.
 * Modified by gwang39 on 10/14/2018
 * Guan Yue Wang
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i : values) {
            sum += i.get();
        }
        context.write(new Text(key), new IntWritable(sum));
    }
}
