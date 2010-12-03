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
package edu.uci.ics.hyracks.examples.onlineaggregation.client;

import java.io.File;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionCountConstraint;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.examples.onlineaggregation.HashPartitioningShuffleConnectorDescriptor;
import edu.uci.ics.hyracks.examples.onlineaggregation.MapperOperatorDescriptor;
import edu.uci.ics.hyracks.examples.onlineaggregation.MarshalledWritable;
import edu.uci.ics.hyracks.examples.onlineaggregation.OnlineInputSplitProviderFactory;
import edu.uci.ics.hyracks.examples.onlineaggregation.ReducerOperatorDescriptor;

public class MapReduceMain {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-conf", usage = "Configuration XML File", required = true)
        public File conf;

        @Option(name = "-num-maps", usage = "Number of Map tasks (default: 1)")
        public int numMaps = 1;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        Configuration conf = new Configuration();
        conf.addResource(options.conf.toURI().toURL());

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job = createJob(options, conf);

        long start = System.currentTimeMillis();
        UUID jobId = hcc.createJob(options.app, job);
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    private static JobSpecification createJob(Options options, Configuration conf) throws Exception {
        JobSpecification spec = new JobSpecification();

        MarshalledWritable<Configuration> mConfig = new MarshalledWritable<Configuration>();
        mConfig.set(conf);
        Job job = new Job(conf);

        spec.setProperty("jobconf", mConfig);

        int jobId = (int) System.currentTimeMillis();

        MapperOperatorDescriptor<Writable, Writable, Writable, Writable> mapper = new MapperOperatorDescriptor<Writable, Writable, Writable, Writable>(
                spec, jobId, mConfig, new OnlineInputSplitProviderFactory());
        mapper.setPartitionConstraint(new PartitionCountConstraint(options.numMaps));

        ReducerOperatorDescriptor<Writable, Writable, Writable, Writable> reducer = new ReducerOperatorDescriptor<Writable, Writable, Writable, Writable>(
                spec, jobId, mConfig);
        reducer.setPartitionConstraint(new PartitionCountConstraint(job.getNumReduceTasks()));

        HashPartitioningShuffleConnectorDescriptor conn = new HashPartitioningShuffleConnectorDescriptor(spec, mConfig);
        spec.connect(conn, mapper, 0, reducer, 0);

        return spec;
    }
}