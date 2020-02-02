package jhu.histogram;

import jhu.enron.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import jhu.avro.EmailSimple;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Guan Yue Wang
 * 2018/11/15
 */
public class EnronHistMapper extends Mapper<AvroKey<EmailSimple>, NullWritable,
        Text, IntWritable> {
    Gson gson = new GsonBuilder().create();
    String inputPath = null;
    IntWritable One = new IntWritable(1);
    String binType = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.binType = context.getConfiguration().get("binType");
    }

    @Override
    protected void map(AvroKey<EmailSimple> key, NullWritable value, Context context) throws IOException, InterruptedException {

        EmailSimple email = key.datum();
        Long longDate = email.getDate();
        String dateText = "";
        
        SimpleDateFormat dfyear = new SimpleDateFormat("yyyy-01-01");
        SimpleDateFormat dfmonth = new SimpleDateFormat("yyyy-MM-01");
        SimpleDateFormat dfday = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dfhour = new SimpleDateFormat("yyyy-MM-dd'T'HH");
        
       
        if(longDate != null)
        {
            Date eDate=new Date(longDate*1000);
            if (binType.equals("year"))
            {
                dateText = dfyear.format(eDate);
            }
            else if (binType.equals("month"))
            {
                dateText = dfmonth.format(eDate);
            }
            else if (binType.equals("day"))
            {
                dateText = dfday.format(eDate);
            }
            else if (binType.equals("hour"))
            {
                dateText = dfhour.format(eDate);
            }
            
            context.write(new Text(dateText), One);
        }
       

    }
}