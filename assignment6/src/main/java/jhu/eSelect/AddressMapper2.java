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
public class AddressMapper2 extends Mapper<AvroKey<EmailSimple>, NullWritable,
        AvroWrapper<EmailSimple>, NullWritable> { 
    Gson gson = new GsonBuilder().create();
    String inputPath = null;
    String regex1 = null;
    String sOption1 = null;
    String regex2 = null;
    String sOption2 = null;
    IntWritable One = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.regex1 = context.getConfiguration().get("regex1");
        this.sOption1 = context.getConfiguration().get("sOption1");
        this.regex2 = context.getConfiguration().get("regex2");
        this.sOption2 = context.getConfiguration().get("sOption2");

    }

    @Override
    protected void map(AvroKey<EmailSimple> key, NullWritable value, Context context) throws IOException, InterruptedException {
    
        EmailSimple email = key.datum();
        
        CharSequence from = email.getFrom();
        List<CharSequence> to = email.getTo();
        List<CharSequence> cc = email.getCc();
        
        boolean includeFlag1 = false;
        boolean includeFlag2 = false;
        
        
        if (sOption1.equals("-from") && from!=null)
        {
                if(from.toString().matches(regex1))
                {
                    includeFlag1 = true;
                }
        }
        else if (sOption1.equals("-to") && to!=null)
        {
            for(CharSequence e : to)
            {
                if(e.toString().matches(regex1))
                {
                    includeFlag1 = true;
                }
            }
                
        }
        else if (sOption1.equals("-cc") && cc!=null)
        {
            for(CharSequence e : cc)
            {
                if(e.toString().matches(regex1))
                {
                    includeFlag1 = true;
                }
            }
            
        }

        
        
        if (sOption2.equals("-from") && from!=null)
        {
                if(from.toString().matches(regex2))
                {
                    includeFlag2 = true;
                }
        }
        else if (sOption2.equals("-to") && to!=null)
        {
            for(CharSequence e : to)
            {
                if(e.toString().matches(regex2))
                {
                    includeFlag2 = true;
                }
            }
                
        }
        else if (sOption2.equals("-cc") && cc!=null)
        {
            for(CharSequence e : cc)
            {
                if(e.toString().matches(regex2))
                {
                    includeFlag2 = true;
                }
            }
            
        }
        
        if (includeFlag1||includeFlag2)
        {
           context.write(new AvroKey<EmailSimple>(email), NullWritable.get());
        }


    }
}
