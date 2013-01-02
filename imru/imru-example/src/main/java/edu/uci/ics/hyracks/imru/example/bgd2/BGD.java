package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.FileOutputStream;
import java.io.PrintWriter;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Generic main class for running Hyracks IMRU jobs.
 * 
 * @author Josh Rosen
 */
public class BGD {
    public static void main(String[] args) throws Exception {
        try {
            if (args.length == 0) {
                args = ("-host localhost"//
                        + " -app bgd"//
                        + " -port 3099"//
                        + " -hadoop-conf /data/imru/hadoop-0.20.2/conf"//
                        + " -agg-tree-type generic"//
                        + " -agg-count 1"//
                        + " -num-rounds 5"//
                        + " -temp-path /tmp"//
                        + " -model-file /tmp/__imru.txt"//
                        + " -example-paths /input/data.txt").split(" ");
            }
            Client<LinearModel, LossGradient> client = new Client<LinearModel, LossGradient>(
                    args);
            Client.disableLogging(); // disable logs during debugging

            // start local cluster controller and two node controller
            // for debugging purpose
            client.startClusterAndNodes();

            // connect to the cluster controller
            client.connect();

            // create the application in local cluster
            client.uploadApp();

//            client.copyFromLocalToHDFS("/data/imru/test/data.txt",
//                    "/input/data.txt");

            BGDJob job = new BGDJob(8000, client.options.numRounds);

            // run job
            JobStatus status = client.run(job);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }

            LinearModel finalModel = client.control.getModel();
            System.out.println("Terminated after "
                    + client.control.getIterationCount() + " iterations");

            System.out
                    .println("Final model [0] " + finalModel.weights.array[0]);
            System.out.println("Final loss was " + finalModel.loss);
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    client.options.modelFilename));
            for (float x : finalModel.weights.array) {
                writer.println("" + x);
            }
            writer.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
