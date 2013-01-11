package edu.uci.ics.hyracks.imru.example.digitRecognition;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Start a local cluster within the process and run the helloworld example.
 * 
 * @author wangrui
 */
public class Main {
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
            cmdline += " -app digitRecognition";
            // hadoop config path
            cmdline += " -hadoop-conf " + System.getProperty("user.home")
                    + "/hadoop-0.20.2/conf";
            // HDFS path to hold intermediate models
            cmdline += " -temp-path /digitRecognition";
            // HDFS path of input data
            cmdline += " -example-paths /mnist/examples.zip";
            // aggregation type
            cmdline += " -agg-tree-type generic";
            // aggregation parameter
            cmdline += " -agg-count 1";
            // don't save intermediate models
            cmdline += " -abondon-intermediate-models";
            // use the same model
            cmdline += " -model-file-name digitRecognition";
            // keep work on one model
            cmdline += " -using-existing-model";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        NeuralNetwork finalModel = Client.run(new Job(), args);
        System.out.println("FinalModel: " + finalModel.errorRate);
        System.exit(0);
    }
}
