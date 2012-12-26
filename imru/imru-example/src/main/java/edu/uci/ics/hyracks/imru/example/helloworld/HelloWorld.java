package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.File;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.api2.IMRUJobControl;
import edu.uci.ics.hyracks.imru.example.bgd.R;
import edu.uci.ics.hyracks.imru.test.ImruTest;

public class HelloWorld {
    public static void main(String[] args) throws Exception {
        try {
            boolean debugging = true; // start everything in one process
            ImruTest.disableLogging();
            String host = "localhost";
            int port = 3099;
            String app = "imru_helloworld";

            // hadoop 0.20.2 need to be started
            String hadoopConfPath = "/data/imru/hadoop-0.20.2/conf";

            // directory in hadoop HDFS which contains intermediate models
            String tempPath = "/helloworld";

            // config files which contains node names
            String clusterConfPath = "imru/imru-core/src/main/resources/conf/cluster.conf";

            // directory in hadoop HDFS which contains input data
            String examplePaths = "/helloworld/input.txt";
            if (debugging) {
                ImruTest.startControllers();
                // Minimum config file to invoke
                // edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUNCBootstrapImpl
                ImruTest.createApp(app, new File(
                        "imru/imru-example/src/main/resources/bootstrap.zip"));
            }
            IMRUJobControl<HelloWorldModel, HelloWorldIncrementalResult> control = new IMRUJobControl<HelloWorldModel, HelloWorldIncrementalResult>();
            control.connect(host, port, hadoopConfPath, clusterConfPath);

            // remove old intermediate models
            FileSystem dfs = FileSystem.get(control.conf);
            if (dfs.listStatus(new Path(tempPath)) != null)
                for (FileStatus f : dfs.listStatus(new Path(tempPath)))
                    dfs.delete(f.getPath());

            // create input file
            dfs.mkdirs(new Path(examplePaths).getParent());
            FSDataOutputStream out = dfs.create(new Path(examplePaths), true);
            out.write("hello world".getBytes());
            out.close();

            // set aggregation type
            // control.selectNAryAggregation(examplePaths, 2);
            control.selectGenericAggregation(examplePaths, 1);

            HelloWorldJob job = new HelloWorldJob();
            JobStatus status = control.run(job, tempPath, app);
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
        System.exit(0);
    }
}
