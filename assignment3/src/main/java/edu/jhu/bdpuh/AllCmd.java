package edu.jhu.bdpuh;

import java.io.BufferedReader;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;


/**
 *
 * Usage: hadoop jar target/<jar-file> <main-class> -all -[<option>]  <src> <dst>
 *
 * Supports the following command and arguments -all [-f] <src> <dst>
 *
 *
 */
public class AllCmd {

    private FileSystem hdfs;
    private String[] args;
    private Options options = new Options();
    private CommandLineParser parser = new BasicParser();


    public AllCmd(FileSystem hdfs, String[] args){

        this.hdfs = hdfs;
        this.args = args;
        options.addOption("all", false, "Copy local file to dfs"
                + " and perform all operations");
    }



    public void execute() throws IOException, ParseException {
        CommandLine line = parser.parse(options, args);
        System.out.println("all cmd detected parsing options..");
        if (line.hasOption("f")) System.out.println("has -f option");

        hdfs.copyFromLocalFile(new Path(args[args.length-1]), new Path(args[args.length-1]));
        
        
        //test!!!hdfs.copyFromLocalFile(new Path(args[args.length-1]), new Path("/"));
        
        
        System.out.println("ls cmd detected parsing options..");

        ArrayList<LocatedFileStatus> listing = new ArrayList<LocatedFileStatus>();
        if (line.hasOption("R")){
            recursiveListing(new Path(args[args.length-1]),listing);
        } else{
            RemoteIterator<LocatedFileStatus> iterator = hdfs.listLocatedStatus(new Path(args[args.length-1]));
            //test!!!RemoteIterator<LocatedFileStatus> iterator = hdfs.listLocatedStatus(new Path("/"));
            while (iterator.hasNext()){
                listing.add(iterator.next());
            }
        }
        printListing(listing, line.hasOption("d"), line.hasOption("h"));
        
        System.out.println("cat cmd detected parsing options..");
        catFile(new Path(args[args.length-1]));
        
        ///!!!test
        
        System.out.println("cat cmd detected parsing options..");
        
        boolean isRemoved =  hdfs.delete(new Path(args[args.length-1]), (line.hasOption("r") || line.hasOption("R")));
        
    }
    
        private void recursiveListing(Path path ,ArrayList<LocatedFileStatus> listing) throws IOException{

        RemoteIterator<LocatedFileStatus> iterator = hdfs.listLocatedStatus(path);
        while(iterator.hasNext()) {
            LocatedFileStatus fs = iterator.next();
            if(fs.isDirectory()){
                listing.add(fs);
                recursiveListing(fs.getPath(), listing);
            } else listing.add(fs);
        }
    }

    private void printListing(ArrayList<LocatedFileStatus> listing, boolean onlyDirs, boolean humanRead) {

        for(LocatedFileStatus fs : listing){
            if(onlyDirs){ // only print directories
                if(fs.isDirectory()){
                    System.out.println((fs.isDirectory() ? "d":"-")+fs.getPermission()+"\t"
                            + (fs.getReplication() == 0? "-": fs.getReplication())+" "+fs.getOwner()+" "+fs.getGroup() +"\t"
                            + (humanRead ? humanReadableFileSize(fs.getLen()) : fs.getLen())+" "+(new Date(fs.getModificationTime()))+"\t"
                            + fs.getPath());
                }
            } else{ // Print everything
                System.out.println((fs.isDirectory() ? "d":"-")+fs.getPermission()+"\t"
                        + (fs.getReplication() == 0? "-": fs.getReplication())+" "+fs.getOwner()+" "+fs.getGroup() +"\t"
                        + (humanRead ? humanReadableFileSize(fs.getLen()) : fs.getLen())+" "+(new Date(fs.getModificationTime()))+"\t"
                        + fs.getPath());
            }

        }
    }

    private String humanReadableFileSize(long size){

        DecimalFormat df = new DecimalFormat("0.00");

        float sizeKb = 1024.0f;
        float sizeMb = sizeKb * sizeKb;
        float sizeGb = sizeMb * sizeKb;
        float sizeTerra = sizeGb * sizeKb;


        if(size < sizeMb)
            return df.format(size / sizeKb)+ " Kb";
        else if(size < sizeGb)
            return df.format(size / sizeMb) + " Mb";
        else if(size < sizeTerra)
            return df.format(size / sizeGb) + " Gb";

        return "Very Very Big";
    }
    
    
        private void catFile(Path path) throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
        try {
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line = br.readLine();
            }
        } finally {
            br.close();
        }

    }

}
