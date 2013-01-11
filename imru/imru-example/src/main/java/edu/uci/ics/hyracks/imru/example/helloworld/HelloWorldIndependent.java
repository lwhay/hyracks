package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.File;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Start a local cluster within the process and run the helloworld example.
 */
public class HelloWorldIndependent {
    public static void main(String[] args) throws Exception {
        // if no argument is given, the following code
        // create default arguments to run the example
        String cmdline = "";
        // debugging mode, everything run in one process
        cmdline += "-debug";
        // disable logging
        cmdline += " -disable-logging";
        // hostname of cluster controller
        cmdline += " -host localhost";
        // port of cluster controller
        cmdline += " -port 3099";
        // application name
        cmdline += " -app helloworld";
        // hadoop config path
        cmdline += " -hadoop-conf " + System.getProperty("user.home")
                + "/hadoop-0.20.2/conf";
        // HDFS path to hold intermediate models
        cmdline += " -temp-path /helloworld";
        // HDFS path of input data
        cmdline += " -example-paths /helloworld/input.txt";
        // aggregation type
        cmdline += " -agg-tree-type generic";
        // aggregation parameter
        cmdline += " -agg-count 1";
        System.out.println("Using command line: " + cmdline);
        args = cmdline.split(" ");
        HelloWorldCluster.main(args);
        System.exit(0);
    }
}
