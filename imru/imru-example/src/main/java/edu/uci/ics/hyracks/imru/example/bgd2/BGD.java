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
        if (args.length == 0) {
            args = ("-host localhost"//
                    + " -app bgd"//
                    + " -port 3099"//
                    + " -hadoop-conf /data/imru/hadoop-0.20.2/conf"//
                    + " -agg-tree-type generic"//
                    + " -agg-count 1"//
                    + " -temp-path /tmp"//
                    + " -example-paths /input/data.txt").split(" ");
        }

        int numRounds = 15;
        String modeFileName = "/tmp/__imru.txt";
        LinearModel finalModel = Client.run(new BGDJob(8000, numRounds), args);
        System.out.println("Final model [0] " + finalModel.weights.array[0]);
        System.out.println("Final loss was " + finalModel.loss);
        PrintWriter writer = new PrintWriter(new FileOutputStream(modeFileName));
        for (float x : finalModel.weights.array)
            writer.println(x);
        writer.close();
        System.exit(0);
    }

}
