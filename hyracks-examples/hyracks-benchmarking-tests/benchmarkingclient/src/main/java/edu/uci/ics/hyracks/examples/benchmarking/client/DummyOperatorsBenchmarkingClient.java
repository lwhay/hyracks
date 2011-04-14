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
package edu.uci.ics.hyracks.examples.benchmarking.client;

import java.util.UUID;
import java.util.regex.Pattern;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInOutOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInputOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInputSinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNRangePartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;

/**
 * @author jarodwen
 */
public class DummyOperatorsBenchmarkingClient {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)", required = false)
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-in-node-splits", usage = "Comma separated list of nodes for the input. A node is <node-name>", required = true)
        public String inNodeSplits;

        @Option(name = "-out-node-splits", usage = "Comma separated list of nodes for the output", required = true)
        public String outNodeSplits;

        @Option(name = "-connector-type", usage = "The type of the connector between operators", required = true)
        public int connectorType;

        @Option(name = "-test-count", usage = "Number of runs for benchmarking")
        public int testCount = 3;

        @Option(name = "-chain-length", usage = "The length of the chain of dummy operators")
        public int chainLength = 1;
    }

    private static final Pattern splitPattern = Pattern.compile(",");

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job;

        System.out.println("Test information:\n" + "ChainLength\tConnectorType\tInNodeSplits\tOutNodeSplits\n"
                + options.chainLength + "\t" + options.connectorType + "\t" + options.inNodeSplits + "\t"
                + options.outNodeSplits);

        System.out.println("\tInitial\tRunning");
        for (int i = 0; i < options.testCount; i++) {
            long start = System.currentTimeMillis();
            job = createJob(options.chainLength, splitPattern.split(options.inNodeSplits),
                    splitPattern.split(options.outNodeSplits), options.connectorType);
            System.out.print(i + "\t" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            UUID jobId = hcc.createJob(options.app, job);
            hcc.start(jobId);
            hcc.waitForCompletion(jobId);
            System.out.println("\t" + (System.currentTimeMillis() - start));
        }
    }

    private static JobSpecification createJob(int chainLength, String[] inNodes, String[] outNodes, int connectorType) {
        JobSpecification spec = new JobSpecification();

        // Dummy operator

        AbstractSingleActivityOperatorDescriptor source, sink;

        if (chainLength == 1) {
            source = new DummyOperatorDescriptor(spec);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, source, inNodes);
            sink = source;
        } else {
            source = new DummyInputOperatorDescriptor(spec);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, source, inNodes);

            AbstractSingleActivityOperatorDescriptor opter = source;

            for (int i = 0; i < chainLength - 2; i++) {
                DummyInOutOperatorDescriptor dummyChain = new DummyInOutOperatorDescriptor(spec);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyChain, inNodes);
                IConnectorDescriptor connChain;
                switch (connectorType) {
                    case 0:
                        connChain = new OneToOneConnectorDescriptor(spec);
                        break;
                    case 1:
                        connChain = new MToNHashPartitioningConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(new int[]{}, new IBinaryHashFunctionFactory[] {}));
                        break;
                    case 2:
                        connChain = new MToNRangePartitioningConnectorDescriptor(spec, 0, null);
                        break;
                    case 3:
                        connChain = new MToNHashPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(new int[]{}, new IBinaryHashFunctionFactory[] {}), new int[]{}, new IBinaryComparatorFactory[]{});
                        break;
                    case 4:
                        connChain = new MToNReplicatingConnectorDescriptor(spec);
                        break;
                    default:
                        connChain = new OneToOneConnectorDescriptor(spec);
                        break;
                }

                spec.connect(connChain, opter, 0, dummyChain, 0);
                opter = dummyChain;
            }

            sink = new DummyInputSinkOperatorDescriptor(spec);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sink, outNodes);

            IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
            spec.connect(conn, opter, 0, sink, 0);
        }

        spec.addRoot(sink);

        return spec;
    }

}
