package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.File;
import java.util.Map;

import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Upload helloworld example to a cluster and run it.
 * 
 * @author wangrui
 */
public class HelloWorldCluster {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // if no argument is given, the following code
            // create default arguments to run the example
            String cmdline = "";
            // hostname of cluster controller
            cmdline += "-host " + Client.getLocalIp();
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
        }

        // create a client object, which handles everything
        Client<HelloWorldModel, HelloWorldIncrementalResult> client = new Client<HelloWorldModel, HelloWorldIncrementalResult>(
                args);

        // connect to the cluster controller
        client.connect();

        // create the application in local cluster
        client.uploadApp();

        // create IMRU job
        HelloWorldJob job = new HelloWorldJob();

        // run job
        System.out.println("start running job");
        JobStatus status = client.run(job);
        System.out.println("job finished");
        if (status == JobStatus.FAILURE) {
            System.err.println("Job failed; see CC and NC logs");
            System.exit(-1);
        }

        // print (or save) the model
        HelloWorldModel finalModel = client.getModel();
        System.out.println("Terminated after "
                + client.control.getIterationCount() + " iterations");
        System.out.println("FinalModel: " + finalModel.totalLength);
    }
}
