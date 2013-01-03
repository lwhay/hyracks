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
            // hostname of cluster controller
            cmdline += "-host localhost";
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

        // create a client object, which handles everything
        Client<NeuralNetwork, Result> client = new Client<NeuralNetwork, Result>(
                args);

        // disable logs
        Client.disableLogging();
        try {
            // start local cluster controller and two node controller
            // for debugging purpose
            client.startClusterAndNodes();

            // connect to the cluster controller
            client.connect();

            // create the application in local cluster
            client.uploadApp();

            // create IMRU job
            Job job = new Job();

            // run job
            JobStatus status = client.run(job);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }

            // print (or save) the model
            NeuralNetwork finalModel = client.getModel();
            System.out.println("Terminated after "
                    + client.control.getIterationCount() + " iterations");
            System.out.println("FinalModel: " + finalModel.errorRate);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        // stop local cluster
        client.deinit();

        // terminate everything
        System.exit(0);
    }
}
