package jhu.searchindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by gwang39 on 10/14/2018.
 * Guan Yue Wang
 */
public class SIReducer extends Reducer<Text, LongWritable, Text, Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String offset = "";
        // concatenate offset
        for(LongWritable i : values) {
            offset += i.get() + "\t" ;
        }
        context.write(new Text(key), new Text(offset));
    }
}
