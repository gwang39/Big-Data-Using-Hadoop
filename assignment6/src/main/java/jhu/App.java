package jhu;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import jhu.avro.EmailSimple;
import jhu.enron.EmailMessage;
import jhu.enron.EnronDriver;
import jhu.histogram.HistDriver;
import jhu.eSelect.TimeDriver1;
import jhu.eSelect.TimeDriver2;
import jhu.eSelect.AddressDriver1;
import jhu.eSelect.AddressDriver2;
import jhu.eSelect.AddressDriver2And;
import jhu.eSelect.AddressDriver3;
import jhu.eSelect.AddressDriver3And;
import jhu.eSelect.SubjectDriver;
import jhu.eSelect.BodyDriver;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.*;

import static com.sun.corba.se.spi.activation.IIOP_CLEAR_TEXT.value;

/**
 * Guan Yue Wang
 * 2018/11/15
 */
public class App extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(ret);
    }

    int writeToAvro(String input, String output) throws IOException {
        FileInputStream fis = new FileInputStream(input);
        DatumWriter<EmailSimple> dr = new SpecificDatumWriter<EmailSimple>(EmailSimple.class);
        DataFileWriter<EmailSimple> dfw = new DataFileWriter<EmailSimple>(dr);
        dfw.create(EmailSimple.SCHEMA$, new File(output));

        //Construct BufferedReader from InputStreamReader
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        Gson gson = new GsonBuilder().create();

        String line = null;
        int total = 0;
        int dateErrors = 0;
        while ((line = br.readLine()) != null) {
            EmailMessage email = gson.fromJson(line, EmailMessage.class);
            EmailSimple emailSimple = email.getEmailSimple();
            if(emailSimple.getDate() == 0L)
                dateErrors++;
            dfw.append(emailSimple);
            total++;
            if((total%1000)==0)
                System.out.printf("%6d records\r",total);
        }
        System.out.printf("%6d records total. %d date parsing errors\n",total, dateErrors);
        dfw.close();
        br.close();

        return 0;
    }

    int runEnronTest(String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("testMode", "avroTest");
        return runEnron(input, output);
    }

    int runEnron(String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new EnronDriver(), new String[]{});
    }
    
    int runHistogram(String bType, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("binType", bType);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new HistDriver(), new String[]{});
    }
    
    int runSelectTime1(String sType, String sOption, String sDate, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("sOption", sOption);
        conf.set("sDate", sDate);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new TimeDriver1(), new String[]{});
    }
    
    int runSelectTime2(String sType, String optionStart, String startDate, String optionEnd, String endDate, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("startDate", startDate);
        conf.set("endDate", endDate);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new TimeDriver2(), new String[]{});
    }
    
    int runSelectSubject(String sType, String patternRegex, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("regex", patternRegex);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new SubjectDriver(), new String[]{});
    }    
    
    int runSelectBody(String sType, String patternRegex, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("regex", patternRegex);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new BodyDriver(), new String[]{});
    }    

    int runSelectAddress1(String sType, String sOption, String patternRegex, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("sOption", sOption);
        conf.set("regex", patternRegex);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new AddressDriver1(), new String[]{});
    }   
    
    int runSelectAddress2(String sType, String sOption1, String patternRegex1,String sOption2, String patternRegex2, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("sOption1", sOption1);
        conf.set("regex1", patternRegex1);
        conf.set("sOption2", sOption2);
        conf.set("regex2", patternRegex2);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new AddressDriver2(), new String[]{});
    }   
    
    int runSelectAddress2And(String sType, String sOption1, String patternRegex1,String sOption2, String patternRegex2, String sAnd, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("sOption1", sOption1);
        conf.set("regex1", patternRegex1);
        conf.set("sOption2", sOption2);
        conf.set("regex2", patternRegex2);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new AddressDriver2And(), new String[]{});
    }   
    
    int runSelectAddress3(String sType, String sOption1, String patternRegex1,String sOption2, String patternRegex2, String sOption3, String patternRegex3, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("sOption1", sOption1);
        conf.set("regex1", patternRegex1);
        conf.set("sOption2", sOption2);
        conf.set("regex2", patternRegex2);
        conf.set("sOption3", sOption3);
        conf.set("regex3", patternRegex3);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new AddressDriver3(), new String[]{});
    }  
    
    int runSelectAddress3And(String sType, String sOption1, String patternRegex1,String sOption2, String patternRegex2,String sOption3, String patternRegex3, String sAnd, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("sType", sType);
        conf.set("sOption1", sOption1);
        conf.set("regex1", patternRegex1);
        conf.set("sOption2", sOption2);
        conf.set("regex2", patternRegex2);
        conf.set("sOption3", sOption2);
        conf.set("regex3", patternRegex2);
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new AddressDriver3And(), new String[]{});
    }   
    
    void showUsage() {
        System.out.println("Usage: ");
        System.out.println("\tenron-avro <localInputPath> <localOutputPath>\ttranslate json to avro EmailSimple record");
        System.out.println("\tenron-stats <inputPath> <outputPath>\trun the enron statistics mapreduce job");
        System.out.println("\tavro-map <inputPath> <outputPath>\trun the avro map only job");
        System.out.println("\temail-select <type> <options> <inputPath> <outputPath> | Select emails based upon the criteria provided.");
        System.out.println("\temail-histogram <bin-type>  <inputPath> <outputPath> \tCreate a histogram based on the bin-type. The bin-type is ```hour```, ```day```, ```month```, or ```year```.");
    }
    

    public int run(String[] strings) throws Exception {
        if(strings.length > 0) {
            if (strings[0].equals("enron-stats") && strings.length == 3) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App enron-stats enron_avro enron_stats
                return runEnron(strings[1], strings[2]);
            } 
            else if(strings[0].equals("enron-avro") && strings.length == 3) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App enron-avro enronemails.json enron_avro
                return writeToAvro(strings[1], strings[2]);
            } 
            else if(strings[0].equals("email-select") && strings[1].equals("time") && strings.length == 6) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select time -start <date> enron_avro start_time_select
                return runSelectTime1(strings[1],strings[2],strings[3],strings[4],strings[5]);
            } 
            else if(strings[0].equals("email-select") && strings[1].equals("time") && strings.length == 8)
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select time -start <date> -end <date> enron_avro start_end_time_select
                return runSelectTime2(strings[1],strings[2],strings[3],strings[4],strings[5],strings[6],strings[7]); 
            } 
            else if(strings[0].equals("email-select") && strings[1].equals("subject") && strings.length == 5) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select subject <pattern> enron_avro subject_select
                return runSelectSubject(strings[1],strings[2],strings[3],strings[4]);            
            } 
            else if(strings[0].equals("email-select") && strings[1].equals("body") && strings.length == 5) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select body <pattern> enron_avro body_select
                return runSelectBody(strings[1],strings[2],strings[3],strings[4]);            
            } 
            else if(strings[0].equals("email-histogram") && strings.length == 4) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-histogram hour enron_avro hour_histogram
                return runHistogram(strings[1],strings[2],strings[3]);
            } 
            else if(strings[0].equals("avro-map") && strings.length == 3) 
            {
                return runEnronTest(strings[1],strings[2]);
            }
            else if(strings[0].equals("email-select") && strings[1].equals("address") && strings.length == 6) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select address -from <pattern> enron_avro from_select
                return runSelectAddress1(strings[1],strings[2],strings[3],strings[4],strings[5]);            
            }
            else if(strings[0].equals("email-select") && strings[1].equals("address") && strings.length == 8) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select address -from <pattern> -to <pattern> enron_avro from_to_select
                return runSelectAddress2(strings[1],strings[2],strings[3],strings[4],strings[5],strings[6],strings[7]);            
            } 
            else if(strings[0].equals("email-select") && strings[1].equals("address") && strings[6].equals("-and") && strings.length == 9) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select address -from <pattern> -to <pattern> enron_avro from_to_select
                return runSelectAddress2And(strings[1],strings[2],strings[3],strings[4],strings[5],strings[6],strings[7],strings[8]);            
            }
            else if(strings[0].equals("email-select") && strings[1].equals("address") && strings.length == 10) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select address -from <pattern> -to <pattern> -cc <pattern> enron_avro from_to_cc_select
                return runSelectAddress3(strings[1],strings[2],strings[3],strings[4],strings[5],strings[6],strings[7],strings[8],strings[9]);            
            } 
            else if(strings[0].equals("email-select") && strings[1].equals("address") && strings[8].equals("-and") && strings.length == 11) 
            {//yarn jar target/assignment6-1.0-SNAPSHOT-jar-with-dependencies.jar jhu.App email-select address -from <pattern> -to <pattern> -cc <pattern> -and enron_avro from_and_to_and_cc_select
                return runSelectAddress3And(strings[1],strings[2],strings[3],strings[4],strings[5],strings[6],strings[7],strings[8],strings[9],strings[10]);            
            }
        }
        showUsage();

        return -1;
    }
}
