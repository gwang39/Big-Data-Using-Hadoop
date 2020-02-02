package jhu.graph;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Guan Yue Wang (gwang39)
 * 2018/10/29
 */
public class GraphMapper extends Mapper<LongWritable, Text, Text, MultipleWritable> {
    Gson gson = new GsonBuilder().create();
    String inputPath = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // read configuration information
        this.inputPath = context.getConfiguration().get("inputPath");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String from = "";
        
        
        if(key.get() == 0) {
            // this is the first line of the file
            // Which files are we processing?
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            context.getCounter("FILE", filename).increment(1);
        }

        // decode json
        EmailMessage message = gson.fromJson(value.toString(), EmailMessage.class);
        for(String k : message.header.keySet()) {
            if(k.equals("From"))
               // context.write(new Text((String)message.header.get(k)), new Text("0 1"));
                from = (String)message.header.get(k);
                
            if(k.equals("To")) 
            {
                List<String> emails = (List<String>)message.header.get(k);
                
                    for(String e : emails)
                    {
                        
                        context.write(new Text(from + " " + e), new MultipleWritable(1,0));
                    }
            }
            
            if(k.equals("Cc")) 
                    {
                        List<String> emails = (List<String>)message.header.get(k);

                        for(String e : emails)
                        {
                             context.write(new Text(from + " " + e), new MultipleWritable(0,1));
                        }
                    }
            }
                

        }




    }

