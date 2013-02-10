/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.example.bgd;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearModel;
import edu.uci.ics.hyracks.imru.example.bgd.deserialized.DeserializedBGDJobSpecification;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.GenericAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NAryAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NoAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;

/**
 * Generic main class for running Hyracks IMRU jobs.
 * 
 * @author Josh Rosen
 */
public class BGDMain {

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

    /**
     * old example, not supported now
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        try {
            if (args.length == 0) {
                args = ("-host localhost"//
                        + " -app bgd"//
                        + " -port 3099"//
                        + " -hadoop-conf /data/imru/hadoop-0.20.2/conf"//
                        + " -agg-tree-type none"//
                        + " -num-rounds 2"//
                        + " -temp-path /tmp"//
                        + " -model-file /tmp/__imru.txt"//
                        + " -cluster-conf imru/imru-core/src/main/resources/conf/cluster.conf"//
                        + " -example-paths /input/data.txt").split(" ");
                //                Client.startCC("localhost", 1099, 3099);
                //                ImruTest.startNC1("nc1", "localhost", 1099);
                //                ImruTest.startNC2("nc2", "localhost", 1099);
                //                ImruTest.createApp("bgd", new File(
                //                        "imru/imru-example/src/main/resources/bootstrap.zip"));
            }
            Options options = new Options();
            CmdLineParser parser = new CmdLineParser(options);
            parser.parseArgument(args);

            HyracksConnection hcc = new HyracksConnection(options.host, options.port);

            if (!new File(options.hadoopConfPath).exists()) {
                System.err.println("Hadoop conf path does not exist!");
                System.exit(-1);
            }
            // Hadoop configuration

            ConfigurationFactory confFactory = new ConfigurationFactory(options.hadoopConfPath);
            Configuration conf = confFactory.createConfiguration();
            FileSystem dfs = FileSystem.get(conf);
            dfs.copyFromLocalFile(new Path("/data/imru/test/data.txt"), new Path("/input/data.txt"));

            // Hyracks cluster configuration
            ClusterConfig.setConfPath(options.clusterConfPath);

            IJobFactory jobFactory;

            if (options.aggTreeType.equals("none")) {
                jobFactory = new NoAggregationIMRUJobFactory(options.examplePaths, confFactory);
            } else if (options.aggTreeType.equals("generic")) {
                if (options.aggCount < 1) {
                    throw new IllegalArgumentException(
                            "Must specify a nonnegative aggregator count using the -agg-count option");
                }
                jobFactory = new GenericAggregationIMRUJobFactory(options.examplePaths, confFactory, options.aggCount);
            } else if (options.aggTreeType.equals("nary")) {
                if (options.fanIn < 1) {
                    throw new IllegalArgumentException("Must specify nonnegative -fan-in");
                }
                jobFactory = new NAryAggregationIMRUJobFactory(options.examplePaths, confFactory, options.fanIn);
            } else {
                throw new IllegalArgumentException("Invalid aggregation tree type");
            }

            LinearModel initalModel = new LinearModel(8000, options.numRounds);
            DeserializedBGDJobSpecification imruSpec = new DeserializedBGDJobSpecification(initalModel, 8000);

            IMRUDriver<LinearModel> driver = new IMRUDriver<LinearModel>(hcc, imruSpec, initalModel, jobFactory, conf,
                    options.tempPath, options.app);

            JobStatus status = driver.run();
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
            int iterationCount = driver.getIterationCount();
            LinearModel finalModel = driver.getModel();
            System.out.println("Terminated after " + iterationCount + " iterations");
            System.out.println("Final model [0] " + finalModel.weights.array[0]);
            System.out.println("Final loss was " + finalModel.loss);
            PrintWriter writer = new PrintWriter(new FileOutputStream(options.modelFilename));
            for (float x : finalModel.weights.array) {
                writer.println("" + x);
            }
            writer.close();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
