package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.File;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.api2.IMRUJobControl;
import edu.uci.ics.hyracks.imru.example.utils.ImruTest;
import edu.uci.ics.hyracks.imru.example.utils.R;

public class HelloWorld {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-selftest", usage = "Path to Hadoop configuration", required = false)
        public boolean selftest = false;

        @Option(name = "-hadoop-conf", usage = "Path to Hadoop configuration", required = true)
        public String hadoopConfPath = "/data/imru/hadoop-0.20.2/conf";

        @Option(name = "-cluster-conf", usage = "Path to Hyracks cluster configuration")
        public String clusterConfPath = "imru/imru-core/src/main/resources/conf/cluster.conf";

        @Option(name = "-temp-path", usage = "HDFS path to hold temporary files", required = true)
        public String tempPath;

        @Option(name = "-examplepath", usage = "HDFS path to hold input data")
        public String examplePath = "/input/data.txt";
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            String cmdline = "-selftest"//
                    + " -host localhost"//
                    + " -port 3099"//
                    + " -app bgd"//
                    + " -hadoop-conf /data/imru/hadoop-0.20.2/conf"//
                    + " -cluster-conf imru/imru-core/src/main/resources/conf/cluster.conf"//
                    + " -temp-path /tmp"//
                    + " -examplepath /input/data.txt";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        try {
            // directory in hadoop HDFS which contains input data
            String examplePaths = "/helloworld/input.txt";
            if (options.selftest) {
                ImruTest.disableLogging();
                ImruTest.startCC("localhost", 1099, 3099);
                ImruTest.startNC1("nc1", "localhost", 1099);
                ImruTest.startNC2("nc2", "localhost", 1099);
                // Minimum config file to invoke
                // edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUNCBootstrapImpl
                ImruTest.createApp(options.app, new File(
                        "imru/imru-example/src/main/resources/bootstrap.zip"));
            }
            IMRUJobControl<HelloWorldModel, HelloWorldIncrementalResult> control = new IMRUJobControl<HelloWorldModel, HelloWorldIncrementalResult>();
            control.connect(options.host, options.port, options.hadoopConfPath,
                    options.clusterConfPath);

            FileSystem dfs = FileSystem.get(control.conf);
            // remove old intermediate models
            // if (dfs.listStatus(new Path(options.tempPath)) != null)
            // for (FileStatus f : dfs.listStatus(new Path(options.tempPath)))
            // dfs.delete(f.getPath());

            // create input file
            dfs.mkdirs(new Path(examplePaths).getParent());
            FSDataOutputStream out = dfs.create(new Path(examplePaths), true);
            out.write("hello world".getBytes());
            out.close();

            // set aggregation type
            // control.selectNAryAggregation(examplePaths, 2);
            control.selectGenericAggregation(examplePaths, 1);

            HelloWorldJob job = new HelloWorldJob();
            JobStatus status = control.run(job, options.tempPath, options.app);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
            int iterationCount = control.getIterationCount();
            HelloWorldModel finalModel = control.getModel();
            System.out.println("Terminated after " + iterationCount
                    + " iterations");
            R.p("FinalModel: " + finalModel.totalLength);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        if (options.selftest)
            System.exit(0);
    }
}
