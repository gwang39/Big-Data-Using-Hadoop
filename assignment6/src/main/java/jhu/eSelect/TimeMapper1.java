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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Text;

/**
 * Created by wilsopw1 on 2/25/17.
 */
public class TimeMapper1 extends Mapper<AvroKey<EmailSimple>, NullWritable,
        AvroWrapper<EmailSimple>, NullWritable> {
    Gson gson = new GsonBuilder().create();
    String inputPath = null;
    String sDate = null;
    String sOption=null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.sDate = context.getConfiguration().get("sDate");
        this.sOption = context.getConfiguration().get("sOption");
    }

    @Override
    protected void map(AvroKey<EmailSimple> key, NullWritable value, Context context) throws IOException, InterruptedException {

            EmailSimple email = key.datum();
            Long elongDate = email.getDate();
            Long slongDate = null;
            
            String datesample = "2002-01-02";
            String timesample = "2002-01-02T08:30:00Z";
            
            SimpleDateFormat dfday = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat dfhour = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            
            Date d;
            
            if(elongDate != null)
            {
            
                if (sDate.length()==datesample.length())
                {
                    try 
                    {
                        d = dfday.parse(sDate);
                        slongDate = d.getTime();
                    } 
                    catch (ParseException ex) 
                    {
                        Logger.getLogger(TimeMapper1.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                else if(sDate.length()==timesample.length())
                {
                    try 
                    {
                        d = dfhour.parse(sDate);
                        slongDate = d.getTime();
                    } 
                    catch (ParseException ex) 
                    {
                        Logger.getLogger(TimeMapper1.class.getName()).log(Level.SEVERE, null, ex);
                    }              
                }



                if (sOption.equals("-start"))
                {
                    if (elongDate*1000 >= slongDate) //email after selected start date
                    {
                        context.write(new AvroKey<EmailSimple>(email), NullWritable.get());
                    }
                }
                
                if((sOption.equals("-end")))
                {
                    if (elongDate*1000 <= slongDate) //email before selected end date
                    {
                        context.write(new AvroKey<EmailSimple>(email), NullWritable.get());
                    }
                }

            }

    }
}
