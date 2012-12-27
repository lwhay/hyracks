package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.api2.IMRUJobControl;
import edu.uci.ics.hyracks.imru.example.utils.ImruTest;

/**
 * Generic main class for running Hyracks IMRU jobs.
 * 
 * @author Josh Rosen
 */
public class BGD {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-example-paths", usage = "Comma separated list of input paths containing training examples.", required = true)
        public String examplePaths;

        @Option(name = "-model-file", usage = "Local file to write the final weights to", required = true)
        public String modelFilename;

        @Option(name = "-temp-path", usage = "HDFS path to hold temporary files", required = true)
        public String tempPath;

        @Option(name = "-hadoop-conf", usage = "Path to Hadoop configuration", required = true)
        public String hadoopConfPath;

        @Option(name = "-cluster-conf", usage = "Path to Hyracks cluster configuration")
        public String clusterConfPath = "cluster.conf";

        @Option(name = "-num-rounds", usage = "The number of iterations to perform", required = true)
        public int numRounds;

        @Option(name = "-agg-tree-type", usage = "The aggregation tree type (none, rack, nary, or generic)", required = true)
        public String aggTreeType;

        @Option(name = "-agg-count", usage = "The number of aggregators to use, if using an aggregation tree")
        public int aggCount = -1;

        @Option(name = "-fan-in", usage = "The fan-in, if using an nary aggregation tree")
        public int fanIn = -1;
    }

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
                        + " -cluster-conf imru/imru-core/src/main/resources/conf/cluster.conf"//
                        + " -example-paths /input/data.txt").split(" ");
                ImruTest.startCC("localhost", 1099, 3099);
                ImruTest.startNC1("nc1", "localhost", 1099);
                ImruTest.startNC2("nc2", "localhost", 1099);
                ImruTest.createApp("bgd", new File(
                        "imru/imru-example/src/main/resources/bootstrap.zip"));
                ImruTest.disableLogging();
            }
            Options options = new Options();
            CmdLineParser parser = new CmdLineParser(options);
            parser.parseArgument(args);

            IMRUJobControl<LinearModel, LossGradient> control = new IMRUJobControl<LinearModel, LossGradient>();
            control.connect(options.host, options.port, options.hadoopConfPath,
                    options.clusterConfPath);

            // copy input files to HDFS
            FileSystem dfs = FileSystem.get(control.conf);
            if (dfs.listStatus(new Path("/tmp")) != null)
                for (FileStatus f : dfs.listStatus(new Path("/tmp")))
                    dfs.delete(f.getPath());
            dfs.copyFromLocalFile(new Path("/data/imru/test/data.txt"),
                    new Path("/input/data.txt"));

            if (options.aggTreeType.equals("none")) {
                control.selectNoAggregation(options.examplePaths);
            } else if (options.aggTreeType.equals("generic")) {
                control.selectGenericAggregation(options.examplePaths,
                        options.aggCount);
            } else if (options.aggTreeType.equals("nary")) {
                control.selectNAryAggregation(options.examplePaths,
                        options.fanIn);
            } else {
                throw new IllegalArgumentException(
                        "Invalid aggregation tree type");
            }

             BGDJob job = new BGDJob(8000, options.numRounds);
             JobStatus status = control.run(job, options.tempPath,
             options.app);
//            BGDJobTmp job = new BGDJobTmp(8000);
//            LinearModel initalModel = new LinearModel(8000, options.numRounds);
//            JobStatus status = control.run(job, initalModel, options.tempPath,
//                    options.app);

            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
            int iterationCount = control.getIterationCount();
            LinearModel finalModel = control.getModel();
            System.out.println("Terminated after " + iterationCount
                    + " iterations");
            System.out
                    .println("Final model [0] " + finalModel.weights.array[0]);
            System.out.println("Final loss was " + finalModel.loss);
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    options.modelFilename));
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
