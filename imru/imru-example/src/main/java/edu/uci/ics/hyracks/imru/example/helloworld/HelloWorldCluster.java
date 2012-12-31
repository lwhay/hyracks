package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.example.utils.Client;

public class HelloWorldCluster {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // default argument to run on local cluster
            Client.generateClusterConfig(new File(
                    "src/main/resources/cluster.conf"), Client.getLocalIp(),
                    Client.getLocalHostName());
            String cmdline = "-host "
                    + Client.getLocalIp()// localhost
                    + " -port 3099"//
                    + " -app helloworld"//
                    + " -hadoop-conf " + System.getProperty("user.home")
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
        try {
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
