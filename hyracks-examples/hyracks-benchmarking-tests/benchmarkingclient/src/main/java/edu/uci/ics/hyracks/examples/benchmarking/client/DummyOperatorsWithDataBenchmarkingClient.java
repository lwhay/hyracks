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

import java.io.File;
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
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DataGeneratorOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyDataProcessingOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInputSinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.IGenDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.ITypeGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.RandomDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.UTF8StringGenerator;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;

/**
 * @author jarodwen
 */
public class DummyOperatorsWithDataBenchmarkingClient {

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
        
        @Option(name = "-out-path", usage = "The prefix (including the path) of the output files")
        public String outPath = System.getProperty("java.io.tmpdir") + "/DummyDataTest_output";

        @Option(name = "-connector-type", usage = "The type of the connector between operators", required = true)
        public int connectorType;

        @Option(name = "-test-count", usage = "Number of runs for benchmarking")
        public int testCount = 3;

        @Option(name = "-chain-length", usage = "The length of the chain of dummy operators")
        public int chainLength = 1;

        @Option(name = "-data-size", usage = "The number of tuples to be generated", required = true)
        public int dataSize;

        @Option(name = "-cardinality", usage = "The cardinality of the data generated", required = true)
        public double cardRatio;

        @Option(name = "-data-gen-fields", usage = "Integers indicating the fields to be generated, separated by comma", required = true)
        public int dataFields;

        @Option(name = "-key-fields", usage = "Key fields of the generated data, separated by comma", required = true)
        public String keyFields;

        @Option(name = "-output-file", usage = "Whether to output the data into a file")
        public boolean isOutputFile = false;
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

        System.out.println("Test information:\n"
                + "ChainLength\tConnectorType\tInNodeSplits\tOutNodeSplits\tDataSize\tCardinality\tDataFields\tKeys\n"
                + options.chainLength + "\t" + options.connectorType + "\t" + options.inNodeSplits + "\t"
                + options.outNodeSplits + "\t" + options.dataSize + "\t" + options.cardRatio + "\t"
                + options.dataFields + "\t" + options.keyFields);

        System.out.println("\tInitial\tRunning");
        
        String[] keys = splitPattern.split(options.keyFields);
        int[] keyFields = new int[keys.length];
        for(int i = 0; i < keys.length; i++){
            keyFields[i] = Integer.valueOf(keys[i]);
        }
        
        for (int i = 0; i < options.testCount; i++) {
            long start = System.currentTimeMillis();
            job = createJob(options.chainLength, splitPattern.split(options.inNodeSplits),
                    splitPattern.split(options.outNodeSplits), options.connectorType, options.dataSize,
                    options.cardRatio, options.dataFields, keyFields, options.isOutputFile, options.outPath);
            System.out.print(i + "\t" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            UUID jobId = hcc.createJob(options.app, job);
            hcc.start(jobId);
            hcc.waitForCompletion(jobId);
            System.out.println("\t" + (System.currentTimeMillis() - start));
        }
    }

    private static JobSpecification createJob(int chainLength, String[] inNodes, String[] outNodes, int connectorType,
            int dataSize, double cardRatio, int dataFields, int[] keyFields, boolean isOutputFile, String outPath) {
        JobSpecification spec = new JobSpecification();

        // Data Generator Operator
        @SuppressWarnings("rawtypes")
        ITypeGenerator[] dataTypeGenerators = new ITypeGenerator[dataFields];
        for (int i = 0; i < dataTypeGenerators.length; i++) {
            dataTypeGenerators[i] = new UTF8StringGenerator(10, true);
        }

        IGenDistributionDescriptor[] dataDistributionDescriptors = new IGenDistributionDescriptor[dataFields];
        for (int i = 0; i < dataDistributionDescriptors.length; i++) {
            dataDistributionDescriptors[i] = new RandomDistributionDescriptor(0, (int) (dataSize * cardRatio));
        }
        
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] fields = new ISerializerDeserializer[dataFields];
        
        for(int i = 0; i < fields.length; i++){
            fields[i] = UTF8StringSerializerDeserializer.INSTANCE;
        }

        RecordDescriptor inRecordDescriptor = new RecordDescriptor(fields);

        DataGeneratorOperatorDescriptor generator = new DataGeneratorOperatorDescriptor(spec, dataTypeGenerators,
                dataDistributionDescriptors, dataSize, true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, generator, inNodes);

        
        IBinaryHashFunctionFactory[] hashFactories = new IBinaryHashFunctionFactory[keyFields.length];
        for(int i = 0; i < keyFields.length; i++){
            hashFactories[i] = UTF8StringBinaryHashFunctionFactory.INSTANCE;
        }
        
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keyFields.length]; 
        for(int i = 0; i < keyFields.length; i++){ 
            comparatorFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
        }

        AbstractSingleActivityOperatorDescriptor opter = generator;

        for (int i = 0; i < chainLength - 2; i++) {

            DummyDataProcessingOperatorDescriptor dummyChain = new DummyDataProcessingOperatorDescriptor(spec,
                    inRecordDescriptor);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyChain, inNodes);

            IConnectorDescriptor connChain;
            switch (connectorType) {
                case 0:
                    connChain = new OneToOneConnectorDescriptor(spec);
                    break;
                case 1:
                    connChain = new MToNHashPartitioningConnectorDescriptor(spec,
                            new FieldHashPartitionComputerFactory(keyFields, hashFactories));
                    break;
                // Currently RangePartition is not supported.
                //case 2:
                //    connChain = new MToNRangePartitioningConnectorDescriptor(spec, 0, null);
                //    break;
                case 3:
                    connChain = new MToNHashPartitioningMergingConnectorDescriptor(spec,
                            new FieldHashPartitionComputerFactory(keyFields, hashFactories), keyFields,
                            comparatorFactories);
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

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, isOutputFile, outNodes, outPath);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, outNodes);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, opter, 0, printer, 0);

        spec.addRoot(printer);

        return spec;
    }

    private static AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, boolean isOutputFile,
            String[] outNodes, String outPath) {
        AbstractSingleActivityOperatorDescriptor printer;

        if (!isOutputFile)
            printer = new DummyInputSinkOperatorDescriptor(spec);
        else
            printer = new PlainFileWriterOperatorDescriptor(spec, new ConstantFileSplitProvider(
                    parseFileSplits(outNodes, outPath)), "\t");

        return printer;
    }
    
    private static FileSplit[] parseFileSplits(String[] outNodes, String outPath) {
        FileSplit[] fSplits = new FileSplit[outNodes.length];
        for (int i = 0; i < outNodes.length; ++i) {
            fSplits[i] = new FileSplit(outNodes[i], new FileReference(new File(outPath + "_" + outNodes[i] + ".txt")));
        }
        return fSplits;
    }

}
