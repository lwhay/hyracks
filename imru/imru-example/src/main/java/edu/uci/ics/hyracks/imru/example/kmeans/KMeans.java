package edu.uci.ics.hyracks.imru.example.kmeans;

import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Start a local cluster within the process and run the kmeans example.
 */
public class KMeans {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
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
            cmdline += " -app kmeans";
            // hadoop config path
            cmdline += " -hadoop-conf " + System.getProperty("user.home")
                    + "/hadoop-0.20.2/conf";
            // HDFS path to hold intermediate models
            cmdline += " -temp-path /kmeans";
            // HDFS path of input data
            cmdline += " -example-paths /kmeans/input.txt";
            // aggregation type
            cmdline += " -agg-tree-type generic";
            // aggregation parameter
            cmdline += " -agg-count 1";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        int k = 3;
        KMeansModel finalModel = Client.run(new KMeansJob(k), args);
        System.out.println("FinalModel:");
        for (int i = 0; i < k; i++)
            System.out.println(" " + finalModel.centroids[i]);
        System.exit(0);
    }
}
