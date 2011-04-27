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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.CountAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.FloatSumAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IFieldValueResultingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.FrameFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalHashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.FloatGenerator;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.IDataGenerator;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.IntegerGenerator;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.UTF8StringGenerator;

public class BenchmarkingClient {
    private static class Options extends BenchmarkingCommonArguments {

        @Option(name = "-hashtable-size", usage = "Number of hashing buckets", required = false)
        public int htSize = 8191;

        @Option(name = "-plain-output", usage = "Whether to output plain text or binary data")
        public boolean outPlain;

        @Option(name = "-test-algorithm", usage = "The algorithm to be tested: 0-external, 1-external sort and precluster, 2-in memory")
        public int testAlg = 0;

        @Option(name = "-generate-data", usage = "Whether to generate data")
        public boolean generateData;

        @Override
        public String getArgumentNames() {
            return super.getArgumentNames() + "htSize\t" + "outPlain\t" + "testAlg\t" + "genData\t";
        }

        @Override
        public String getArgumentValues() {
            return super.getArgumentValues() + htSize + "\t" + outPlain + "\t" + testAlg + "\t" + generateData + "\t";
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job;
        System.out.println(options.getArgumentNames() + "\n" + options.getArgumentValues());
        System.out.println("\tInitial\tRunning");
        for (int i = 0; i < options.testCount; i++) {
            long start = System.currentTimeMillis();
            job = createJob(
                    options.generateData ? generateData(options.dataSize, options.cardRatio, options.inNodeSplits)
                            : parseFileSplits(options.inNodeSplits), parseFileSplits(options.outNodeSplits),
                    options.testAlg, options.frameLimit, options.htSize, options.outPlain);
            System.out.print(i + "\t" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            UUID jobId = hcc.createJob(options.app, job);
            hcc.start(jobId);
            hcc.waitForCompletion(jobId);
            System.out.println("\t" + (System.currentTimeMillis() - start));
        }
    }

    private static FileSplit[] generateData(long dataSize, double uniqueRatio, String fileSplits) throws IOException {
        System.out.println("***** Generating data...");
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        // The range of keys
        long keyRange = (long) (uniqueRatio * dataSize);
        Random rand = new Random();
        // Number of lines in a file split
        long fileLength = (dataSize - dataSize % splits.length) / splits.length;
        // Generate data
        // Initialize generators
        @SuppressWarnings("rawtypes")
        IDataGenerator[] generators = new IDataGenerator[] { UTF8StringGenerator.getSingleton(),
                IntegerGenerator.getSingleton(), FloatGenerator.getSingleton(), UTF8StringGenerator.getSingleton() };
        long lcnt = 0;
        BufferedWriter writer;
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int idx = s.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("File split " + s + " not well formed");
            }
            writer = new BufferedWriter(new FileWriter(s.substring(idx + 1)));
            while (lcnt < dataSize && lcnt / fileLength == i) {
                writer.append(((UTF8StringGenerator) generators[0]).generate((long) (rand.nextFloat() * keyRange), 20)
                        + "|");
                writer.append(((IntegerGenerator) generators[1]).generate(20) + "|");
                writer.append(String.valueOf(generators[2].generate()) + "|");
                writer.append(((UTF8StringGenerator) generators[3]).generate(80) + "\n");
                lcnt++;
            }
            writer.close();
            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1))));
        }
        return fSplits;
    }

    private static FileSplit[] parseFileSplits(String fileSplits) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int idx = s.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("File split " + s + " not well formed");
            }
            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1))));
        }
        return fSplits;
    }

    private static JobSpecification createJob(FileSplit[] inSplits, FileSplit[] outSplits, int alg, int framesLimit,
            int htSize, boolean outPlain) {
        JobSpecification spec = new JobSpecification();

        IFileSplitProvider inSplitsProvider = new ConstantFileSplitProvider(inSplits);
        RecordDescriptor inDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor inScanner = new FileScanOperatorDescriptor(spec, inSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE },
                        '|'), inDesc);
        createPartitionConstraint(spec, inScanner, inSplits);

        RecordDescriptor outDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE });

        int[] keys = new int[] { 0 };

        AbstractOperatorDescriptor grouper;

        switch (alg) {
            case 0: // External hash group
                grouper = new ExternalHashGroupOperatorDescriptor(spec, keys, framesLimit, false,
                        new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                        new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                        new MultiAggregatorFactory(new IFieldValueResultingAggregatorFactory[] {
                                new CountAggregatorFactory(), new FloatSumAggregatorFactory(2) }), outDesc, htSize);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect scanner with the grouper
                IConnectorDescriptor scanGroupConn = new MToNHashPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanGroupConn, inScanner, 0, grouper, 0);
                break;
            case 1: // External sort + pre-cluster
                ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, keys,
                        new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, inDesc);
                createPartitionConstraint(spec, sorter, inSplits);

                // Connect scan operator with the sorter
                IConnectorDescriptor scanSortConn = new MToNHashPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanSortConn, inScanner, 0, sorter, 0);

                grouper = new PreclusteredGroupOperatorDescriptor(spec, keys,
                        new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                        new MultiAggregatorFactory(new IFieldValueResultingAggregatorFactory[] {
                                new CountAggregatorFactory(), new FloatSumAggregatorFactory(2) }), outDesc);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect sorter with the pre-cluster
                OneToOneConnectorDescriptor sortGroupConn = new OneToOneConnectorDescriptor(spec);
                spec.connect(sortGroupConn, sorter, 0, grouper, 0);
                break;
            case 2: // In-memory hash group
                grouper = new HashGroupOperatorDescriptor(spec, keys, new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                        new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                        new MultiAggregatorFactory(new IFieldValueResultingAggregatorFactory[] {
                                new CountAggregatorFactory(), new FloatSumAggregatorFactory(2) }), outDesc, htSize);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect scanner with the grouper
                IConnectorDescriptor scanConn = new MToNHashPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanConn, inScanner, 0, grouper, 0);
                break;
            default:
                grouper = new ExternalHashGroupOperatorDescriptor(spec, keys, framesLimit, false,
                        new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                        new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                        new MultiAggregatorFactory(
                                new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }), outDesc,
                        htSize);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect scanner with the grouper
                IConnectorDescriptor scanGroupConnDef = new MToNHashPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanGroupConnDef, inScanner, 0, grouper, 0);
        }

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(outSplits);

        AbstractSingleActivityOperatorDescriptor writer;

        if (outPlain)
            writer = new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, "|");
        else
            writer = new FrameFileWriterOperatorDescriptor(spec, outSplitProvider);

        createPartitionConstraint(spec, writer, outSplits);

        IConnectorDescriptor groupOutConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(groupOutConn, grouper, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }

    private static void createPartitionConstraint(JobSpecification spec, IOperatorDescriptor op, FileSplit[] splits) {
        String[] parts = new String[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            parts[i] = splits[i].getNodeName();
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, parts);
    }
}