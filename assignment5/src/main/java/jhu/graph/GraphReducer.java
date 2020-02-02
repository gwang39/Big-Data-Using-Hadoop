/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jhu.graph;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Guan Yue Wang (gwang39)
 * 2018/10/29
 */
public class GraphReducer extends Reducer<Text, MultipleWritable, Text, MultipleWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<MultipleWritable> values, Context context) throws IOException, InterruptedException {
        MultipleWritable output = new MultipleWritable();
        
        for(MultipleWritable value: values) {
            output.merge(value);
        }
        context.write(new Text(key), output);
    }
}
