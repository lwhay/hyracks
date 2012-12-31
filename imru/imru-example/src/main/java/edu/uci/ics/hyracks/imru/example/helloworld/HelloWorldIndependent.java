package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.File;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.example.utils.Client;

public class HelloWorldIndependent {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // default argument to run the example
            Client.generateClusterConfig(new File(
                    "src/main/resources/cluster.conf"), "127.0.0.1", "nc1",
                    "127.0.0.1", "nc2");
            String cmdline = "-host localhost"//
                    + " -port 3099"//
                    + " -app helloworld"//
                    + " -hadoop-conf "
                    + System.getProperty("user.home")
                    + "/hadoop-0.20.2/conf"//
                    + " -cluster-conf src/main/resources/cluster.conf"//
                    + " -temp-path /helloworld"//
                    + " -example-paths /helloworld/input.txt"//
                    + " -agg-tree-type generic"//
                    + " -agg-count 1";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }
        Client<HelloWorldModel, HelloWorldIncrementalResult> client = new Client<HelloWorldModel, HelloWorldIncrementalResult>(
                args);
        Client.disableLogging(); // disable logs during debugging
        try {
            // start local cluster controller and two node controller
            // for debugging purpose
            client.startClusterAndNodes();

            // connect to the cluster controller
            client.connect();

            // create the application in local cluster
            client.uploadApp();

            // create IMRU job
            HelloWorldJob job = new HelloWorldJob();

            // run job
            JobStatus status = client.run(job);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }

            // print (or save) the model
            HelloWorldModel finalModel = client.control.getModel();
            System.out.println("Terminated after "
                    + client.control.getIterationCount() + " iterations");
            System.out.println("FinalModel: " + finalModel.totalLength);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
