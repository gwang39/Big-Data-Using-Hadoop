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
public class TimeMapper2 extends Mapper<AvroKey<EmailSimple>, NullWritable,
        AvroWrapper<EmailSimple>, NullWritable> {
    Gson gson = new GsonBuilder().create();
    String inputPath = null;
    String startDate=null;
    String endDate=null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.startDate = context.getConfiguration().get("startDate");
        this.endDate = context.getConfiguration().get("endDate");
    }

    @Override
    protected void map(AvroKey<EmailSimple> key, NullWritable value, Context context) throws IOException, InterruptedException {

            EmailSimple email = key.datum();
            Long elongDate = email.getDate();
            Long startlongDate = null;
            Long endlongDate = null;

            String datesample = "2002-01-02";
            String timesample = "2002-01-02T08:30:00Z";
            
            SimpleDateFormat dfday = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat dfhour = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            
            Date d1;
            Date d2;
            
            if(elongDate != null)
            {
            
                if (startDate.length()==datesample.length())
                {
                    try 
                    {
                        d1 = dfday.parse(startDate);
                        startlongDate = d1.getTime();
                    } 
                    catch (ParseException ex) 
                    {
                        Logger.getLogger(TimeMapper2.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                else if(startDate.length()==timesample.length())
                {
                    try 
                    {
                        d1 = dfhour.parse(startDate);
                        startlongDate = d1.getTime();
                    } 
                    catch (ParseException ex) 
                    {
                        Logger.getLogger(TimeMapper2.class.getName()).log(Level.SEVERE, null, ex);
                    }              
                }

                if (endDate.length()==datesample.length())
                {
                    try 
                    {
                        d2 = dfday.parse(endDate);
                        endlongDate = d2.getTime();
                    } 
                    catch (ParseException ex) 
                    {
                        Logger.getLogger(TimeMapper2.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                else if(endDate.length()==timesample.length())
                {
                    try 
                    {
                        d2 = dfhour.parse(endDate);
                        endlongDate = d2.getTime();
                    } 
                    catch (ParseException ex) 
                    {
                        Logger.getLogger(TimeMapper2.class.getName()).log(Level.SEVERE, null, ex);
                    }              
                }


                
                if ((elongDate*1000 >= startlongDate)&& (elongDate*1000 <= endlongDate))//email after selected start date
                {
                        context.write(new AvroKey<EmailSimple>(email), NullWritable.get());
                }
               
                


            }

    }
}
