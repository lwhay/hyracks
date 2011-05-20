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
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.IntegerBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IFieldValueResultingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IntSumAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.SumAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DataGeneratorOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.IGenDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.ITypeGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.IntegerGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.RandomDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.SequentialIDDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.TupleRangePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.UTF8StringGenerator;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.BSTSpillableGroupingTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalHashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableGroupingTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;

/**
 * @author jarodwen
 */
public class AggregatorsBenchmarkingClient {

    private static class Options extends BenchmarkingCommonArguments {

        @Option(name = "-out-path", usage = "The prefix (including the path) of the output files")
        public String outPath = System.getProperty("java.io.tmpdir") + "/AggregatorsTest_output";

        @Option(name = "-aggregator-type", usage = "Aggregator (algorithm) to be used", required = true)
        public int aggregatorType;

        @Option(name = "-hashtable-size", usage = "Hash table size (default: 8191)", required = false)
        public int htSize = 8191;
        
        @Option(name = "-fixed-data-size", usage = "Generate fixed size of the data (so each node generate only a potion of the data)")
        public boolean isDataSizeFixed;
        
        @Option(name = "-one-to-one-out", usage = "Use one-to-one connector for outputting results")
        public boolean isOneToOneOutput;

        @Override
        public String getArgumentNames() {
            return super.getArgumentNames() + "outPath\t" + "aggType\t" + "htSize\t" + "isDataFixed\t" + "isOneToOneOut";
        }

        @Override
        public String getArgumentValues() {
            return super.getArgumentValues() + outPath + "\t" + aggregatorType + "\t" + htSize + "\t" + isDataSizeFixed + "\t" + isOneToOneOutput;
        }
    }

    private static final Pattern splitPattern = Pattern.compile(",");

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job;

        System.out.println(options.getArgumentNames() + "\n" + options.getArgumentValues());

        System.out.println("\tInitial\tRunning");

        String[] keys = splitPattern.split(options.keyFields);
        int[] keyFields = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keyFields[i] = Integer.valueOf(keys[i]);
        }

        for (int i = 0; i < options.testCount; i++) {
            long start = System.currentTimeMillis();
            job = createJob(options.aggregatorType, splitPattern.split(options.inNodeSplits),
                    splitPattern.split(options.outNodeSplits), options.dataSize, options.tupleLength,
                    options.cardRatio, options.dataFields, keyFields, options.outPath, options.frameLimit,
                    options.htSize, options.randSeed, options.repeatable, options.isDataSizeFixed, options.isOneToOneOutput);
            System.out.print(i + "\t" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            UUID jobId = hcc.createJob(options.app, job);
            hcc.start(jobId);
            hcc.waitForCompletion(jobId);
            System.out.println("\t" + (System.currentTimeMillis() - start));
        }
    }

    private static JobSpecification createJob(int aggregatorType, String[] inNodes, String[] outNodes, int dataSize,
            int tupleLength, double cardRatio, int dataFields, int[] keyFields, String outPath, int frameLimit,
            int htSize, int randSeed, boolean repeatable, boolean isDataFixed, boolean oneToOneOutput) throws Exception {
        JobSpecification spec = new JobSpecification();

        // Data Generator Operator
        @SuppressWarnings("rawtypes")
        // Generate fields
        ITypeGenerator[] dataTypeGenerators = new ITypeGenerator[dataFields];
        
        // Generate an id field
        dataTypeGenerators[0] = new IntegerGenerator((isDataFixed ? dataSize / inNodes.length : dataSize), randSeed);
        
        for (int i = 1; i < dataTypeGenerators.length - 1; i++) {
            dataTypeGenerators[i] = new UTF8StringGenerator(tupleLength, true, randSeed + i);
        }
        // Generate an integer field 
        dataTypeGenerators[dataTypeGenerators.length - 1] = new IntegerGenerator((isDataFixed ? dataSize / inNodes.length : dataSize), randSeed
                + dataTypeGenerators.length);

        // Distribution controllers
        // - Integer ID field
        IGenDistributionDescriptor[] dataDistributionDescriptors = new IGenDistributionDescriptor[dataFields];
        dataDistributionDescriptors[0] = new SequentialIDDistributionDescriptor();
        // - Other fields as data load
        for (int i = 1; i < dataDistributionDescriptors.length; i++) {
            boolean isKey = false;
            for (int j = 0; j < keyFields.length; j++) {
                if (keyFields[j] == i) {
                    isKey = true;
                    break;
                }
            }
            if (isKey)
                dataDistributionDescriptors[i] = new RandomDistributionDescriptor((int) ((isDataFixed ? dataSize / inNodes.length : dataSize) * cardRatio));
            else
                dataDistributionDescriptors[i] = new RandomDistributionDescriptor();
        }

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] fields = new ISerializerDeserializer[dataFields];
        fields[0] = IntegerSerializerDeserializer.INSTANCE;
        
        for (int i = 1; i < fields.length - 1; i++) {
            fields[i] = UTF8StringSerializerDeserializer.INSTANCE;
        }
        fields[fields.length - 1] = IntegerSerializerDeserializer.INSTANCE;

        RecordDescriptor inRecordDescriptor = new RecordDescriptor(fields);

        DataGeneratorOperatorDescriptor generator = new DataGeneratorOperatorDescriptor(spec, dataTypeGenerators,
                dataDistributionDescriptors, (isDataFixed ? dataSize / inNodes.length : dataSize), true, randSeed, repeatable);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, generator, inNodes);

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] outFields = new ISerializerDeserializer[keyFields.length + 1];
        for (int i = 0; i < keyFields.length; i++) {
            if(i == 0)
                outFields[i] = IntegerSerializerDeserializer.INSTANCE;
            else
                outFields[i] = UTF8StringSerializerDeserializer.INSTANCE;
        }
        outFields[outFields.length - 1] = IntegerSerializerDeserializer.INSTANCE;

        RecordDescriptor outRecordDescriptor = new RecordDescriptor(outFields);

        IBinaryHashFunctionFactory[] hashFactories = new IBinaryHashFunctionFactory[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            if (i != dataFields - 1 && i != 0)
                hashFactories[i] = UTF8StringBinaryHashFunctionFactory.INSTANCE;
            else
                hashFactories[i] = IntegerBinaryHashFunctionFactory.INSTANCE;
        }

        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keyFields.length];
        
        for (int i = 0; i < keyFields.length; i++) {
            if (i != dataFields - 1 && i != 0)
                comparatorFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
            else
                comparatorFactories[i] = IntegerBinaryComparatorFactory.INSTANCE;
        }

        AbstractOperatorDescriptor grouper;

        switch (aggregatorType) {
            case 0:
                // Precluster + aggregator
                ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, frameLimit, keyFields,
                        comparatorFactories, inRecordDescriptor);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, inNodes);

                // Connect generator with the grouper
                IConnectorDescriptor scanSortConn = new MToNHashPartitioningConnectorDescriptor(spec,
                        new TupleRangePartitionComputerFactory());
                spec.connect(scanSortConn, generator, 0, sorter, 0);

                // Aggregator operator
                grouper = new PreclusteredGroupOperatorDescriptor(
                        spec,
                        keyFields,
                        comparatorFactories,
                        new MultiAggregatorFactory(
                                new IFieldValueResultingAggregatorFactory[] { new SumAggregatorFactory(dataFields - 1) }),
                        outRecordDescriptor);

                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, inNodes);

                OneToOneConnectorDescriptor sortGroupConn = new OneToOneConnectorDescriptor(spec);
                spec.connect(sortGroupConn, sorter, 0, grouper, 0);
                break;

            case 1:
                // External hash group, previous version
                grouper = new ExternalHashGroupOperatorDescriptor(
                        spec,
                        keyFields,
                        frameLimit,
                        true,
                        new FieldHashPartitionComputerFactory(keyFields, hashFactories),
                        comparatorFactories,
                        new MultiAggregatorFactory(
                                new IFieldValueResultingAggregatorFactory[] { new SumAggregatorFactory(dataFields - 1) }),
                        outRecordDescriptor, htSize);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, inNodes);

                IConnectorDescriptor genGroupConn = new MToNHashPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keyFields, hashFactories)
                        //new TupleRangePartitionComputerFactory()
                );
                spec.connect(genGroupConn, generator, 0, grouper, 0);
                break;

            case 2:
                // External hash group, refacotored version
                grouper = new ExternalGroupOperatorDescriptor(spec, keyFields, frameLimit, comparatorFactories,
                        new MultiAggregatorDescriptorFactory(
                                new IAggregatorDescriptorFactory[] { new IntSumAggregatorDescriptorFactory(
                                        dataFields - 1) }), outRecordDescriptor, new HashSpillableGroupingTableFactory(
                                new FieldHashPartitionComputerFactory(keyFields, hashFactories), htSize), false);

                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, inNodes);

                IConnectorDescriptor genHashGroupConn = new MToNHashPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keyFields, hashFactories)
                        //new TupleRangePartitionComputerFactory()
                );
                spec.connect(genHashGroupConn, generator, 0, grouper, 0);
                break;

            case 3:
                // External binary group, refactored version
                grouper = new ExternalGroupOperatorDescriptor(spec, keyFields, frameLimit, comparatorFactories,
                        new MultiAggregatorDescriptorFactory(
                                new IAggregatorDescriptorFactory[] { new IntSumAggregatorDescriptorFactory(
                                        dataFields - 1) }), outRecordDescriptor,
                        new BSTSpillableGroupingTableFactory(), false);

                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, inNodes);

                IConnectorDescriptor genBinGroupConn = new MToNHashPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keyFields, hashFactories)
                        //new TupleRangePartitionComputerFactory()
                );
                spec.connect(genBinGroupConn, generator, 0, grouper, 0);
                break;
            default:
                // Directly output the data
                oneToOneOutput = true;
                grouper = generator;
        }

        if (oneToOneOutput) {
            
            PlainFileWriterOperatorDescriptor dprinter = new PlainFileWriterOperatorDescriptor(spec,
                    new ConstantFileSplitProvider(parseFileSplits(outNodes, outPath, dataSize + "_"
                            + aggregatorType)), "\t");
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dprinter, outNodes);

            IConnectorDescriptor directPrintConn = new OneToOneConnectorDescriptor(spec);
            spec.connect(directPrintConn, grouper, 0, dprinter, 0);

            spec.addRoot(dprinter);
            
        } else {
            PlainFileWriterOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                    new ConstantFileSplitProvider(parseFileSplits(outNodes, outPath, dataSize + "_" + aggregatorType)),
                    "\t");
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, outNodes);

            IConnectorDescriptor groupPrintConn = new MToNHashPartitioningMergingConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields,
                            hashFactories),
                    keyFields, comparatorFactories);
            spec.connect(groupPrintConn, grouper, 0, printer, 0);

            spec.addRoot(printer);
        }
        return spec;
    }

    private static FileSplit[] parseFileSplits(String[] outNodes, String outPath, String suffix) {
        FileSplit[] fSplits = new FileSplit[outNodes.length];
        for (int i = 0; i < outNodes.length; ++i) {
            fSplits[i] = new FileSplit(outNodes[i], new FileReference(new File(outPath + "_" + outNodes[i] + "_"
                    + System.currentTimeMillis() + "_" + suffix + ".txt")));
        }
        return fSplits;
    }

}
