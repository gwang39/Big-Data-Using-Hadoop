package jhu;

import jhu.enron.EnronDriver;
import jhu.graph.GraphDriver;
import jhu.central.CentralDriver;
import jhu.centraloption.CentralOptionDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

/**
 * Programming Assignment: Map Reduce Wrangling
 * Created by Guan Yue Wang (gwang39)
 * 2018/10/29
 */
public class App extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception {
        PropertyConfigurator.configure("/etc/hadoop/log4j.properties");
        int ret = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(ret);
    }

    int runEnron(String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new EnronDriver(), new String[]{});
    }
    
    int runGraph(String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new GraphDriver(), new String[]{});
    }
    
    int runCentral(String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        return ToolRunner.run(conf, new CentralDriver(), new String[]{});
    }
    
    int runCentralwOption(String tOption, String tNum, String input, String output) throws Exception {
        Configuration conf = getConf();
        conf.set("inputPath", input);
        conf.set("outputPath", output);
        conf.set("tOption", tOption);
        conf.set("tNum", tNum);
        return ToolRunner.run(conf, new CentralOptionDriver(), new String[]{});
    }

    void showUsage() {
        System.out.println("Usage: ");
        System.out.println("\tenron-stats <inputPath> <outputPath>\trun the enron statistics mapreduce job");
        System.out.println("\tenron-graph <inputPath> <outputPath>\tbuilds the edge lists with properties");
        System.out.println("\tdegree-centrality <inputPath> <outputPath>\tcalculate degree centrality of each node in your graph");
        System.out.println("\tdegree-centrality -ct/-tt x <inputPath> <outputPath>\tcalculate degree centrality of each node in your graph with cc/to threshold x");
        
        
    }

    public int run(String[] strings) throws Exception {
        if(strings.length > 0) {
            if (strings[0].equals("enron-stats") && strings.length == 3) {
                return runEnron(strings[1], strings[2]);
            }
            else if (strings[0].equals("enron-graph") && strings.length == 3) {
                return runGraph(strings[1], strings[2]);
            }
            else if (strings[0].equals("degree-centrality") && strings.length == 3) {
                return runCentral(strings[1], strings[2]);
            }
            else if (strings[0].equals("degree-centrality") && (strings[1].equals("-ct")||strings[1].equals("-tt")) && strings.length == 5) {
                return runCentralwOption(strings[1], strings[2], strings[3], strings[4]);
            
            }
        }

        showUsage();

        return -1;
    }
}
