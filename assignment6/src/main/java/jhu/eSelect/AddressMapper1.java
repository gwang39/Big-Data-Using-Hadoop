package jhu.eSelect;

import jhu.histogram.*;
import jhu.enron.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jhu.avro.EmailSimple;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Guan Yue Wang
 * 2018/11/15
 */
public class AddressMapper1 extends Mapper<AvroKey<EmailSimple>, NullWritable,
        AvroWrapper<EmailSimple>, NullWritable> { 
    Gson gson = new GsonBuilder().create();
    String inputPath = null;
    String regex = null;
    String sOption = null;
    IntWritable One = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.regex = context.getConfiguration().get("regex");
        this.sOption = context.getConfiguration().get("sOption");

    }

    @Override
    protected void map(AvroKey<EmailSimple> key, NullWritable value, Context context) throws IOException, InterruptedException {
    
        EmailSimple email = key.datum();
        
        CharSequence from = email.getFrom();
        List<CharSequence> to = email.getTo();
        List<CharSequence> cc = email.getCc();
        
        boolean includeFlag = false;
        
        
        if (sOption.equals("-from") && from!=null)
        {
                if(from.toString().matches(regex))
                {
                    includeFlag = true;
                }
        }
        else if (sOption.equals("-to") && to!=null)
        {
            for(CharSequence e : to)
            {
                if(e.toString().matches(regex))
                {
                    includeFlag = true;
                }
            }
                
        }
        else if (sOption.equals("-cc") && cc!=null)
        {
            for(CharSequence e : cc)
            {
                if(e.toString().matches(regex))
                {
                    includeFlag = true;
                }
            }
            
        }
                       
        
        if (includeFlag)
        {
           context.write(new AvroKey<EmailSimple>(email), NullWritable.get());
        }


    }
}
