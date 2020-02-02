/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jhu.centraloption;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

/**
 * Created by Guan Yue Wang (gwang39)
 * 2018/10/29
 */
public class CentralOptionMapper extends Mapper<LongWritable, Text, Text, MultipleWritable> {

    String inputPath = null;
    String tOption = null;
    int tNum = 0;
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        // read configuration information
        this.inputPath = context.getConfiguration().get("inputPath");
        this.tOption = context.getConfiguration().get("tOption");
        this.tNum = Integer.parseInt(context.getConfiguration().get("tNum"));

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        int currentCC = 0;
        int currentTO = 0;
        int thresholdCC = 0;
        int thresholdTO = 0;
        


        
        if(key.get() == 0) {
            // this is the first line of the file
            // Which files are we processing?
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            context.getCounter("FILE", filename).increment(1);
        }
        
        String[] words = value.toString().split("\\s+");
        
        if(tOption.equals("-ct"))
        {
            thresholdCC = tNum;
        }
        else if(tOption.equals("-tt"))
        {
            thresholdTO = tNum;
        }
        
        currentTO = Integer.parseInt(words[2]);
        currentCC = Integer.parseInt(words[3]);
        
        if ((currentTO>=thresholdTO) && (currentCC>=thresholdCC))
        {
            context.write(new Text(words[0]), new MultipleWritable(0,1));
            context.write(new Text(words[1]), new MultipleWritable(1,0));
        }

    }

    
}
