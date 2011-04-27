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
package edu.uci.ics.hyracks.tests.benchmarking;

import java.io.File;

import org.junit.Test;

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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.ConcatAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.CountAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IntSumAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DataGeneratorOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.IGenDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.ITypeGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.IntegerGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.RandomDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.UTF8StringGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.ZipfDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableGroupingTableFactory;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

/**
 * @author jarodwen
 */
public class BenchmarkingDataGenTests extends AbstractIntegrationTest {

    final String[] outSplits = new String[] { "nc1:" + System.getProperty("java.io.tmpdir") + "/data_nc1",
            "nc2:" + System.getProperty("java.io.tmpdir") + "/data_nc2" };

    final int dataSize = 1000;

    final double cardRatio = 0.4;

    final boolean outToFile = false;

    final int randSeed = 83748;

    private static FileSplit[] parseFileSplits(String[] splits) {
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

    @Test
    public void twoSimpleFieldsDataGenTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        @SuppressWarnings("rawtypes")
        ITypeGenerator[] dataGenerators = new ITypeGenerator[] { new UTF8StringGenerator(10, true, randSeed),
                new IntegerGenerator(97, 100000, randSeed + 1), new UTF8StringGenerator(20, false, randSeed + 2),
                new IntegerGenerator(randSeed + 3) };

        IGenDistributionDescriptor[] genDistributionDescriptors = new IGenDistributionDescriptor[] {
                new RandomDistributionDescriptor((int) (dataSize * cardRatio)),
                new RandomDistributionDescriptor((int) (dataSize * cardRatio)),
                new ZipfDistributionDescriptor(dataSize, 1),
                new ZipfDistributionDescriptor((int) (dataSize * cardRatio), 0.5) };

        DataGeneratorOperatorDescriptor generator = new DataGeneratorOperatorDescriptor(spec, dataGenerators,
                genDistributionDescriptors, dataSize, false, randSeed + 4, true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, generator, NC2_ID, NC1_ID);

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(parseFileSplits(outSplits));

        AbstractSingleActivityOperatorDescriptor printer;

        if (outToFile)
            printer = new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, "\t");
        else
            printer = new PrinterOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, generator, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void genAndAggregateTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Data generator operator

        @SuppressWarnings("rawtypes")
        ITypeGenerator[] dataGenerators = new ITypeGenerator[] { new UTF8StringGenerator(10, true, randSeed),
                new IntegerGenerator(97, 93748, randSeed + 1), new UTF8StringGenerator(20, false, randSeed + 2),
                new IntegerGenerator(randSeed + 3), new UTF8StringGenerator(3, false, randSeed + 4) };

        IGenDistributionDescriptor[] genDistributionDescriptors = new IGenDistributionDescriptor[] {
                new RandomDistributionDescriptor((int) (dataSize * cardRatio)),
                new RandomDistributionDescriptor((int) (dataSize * cardRatio)),
                new ZipfDistributionDescriptor((int) (dataSize * cardRatio), 1),
                new ZipfDistributionDescriptor((int) (dataSize * cardRatio), 0.5), new RandomDistributionDescriptor() };

        DataGeneratorOperatorDescriptor generator = new DataGeneratorOperatorDescriptor(spec, dataGenerators,
                genDistributionDescriptors, dataSize, true, randSeed + 5, true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, generator, NC2_ID, NC1_ID);

        // External group operator

        RecordDescriptor outRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 2 };
        int frameLimits = 3;
        int tableSize = 8;

        ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(spec, keyFields, frameLimits,
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                new MultiAggregatorDescriptorFactory(new IAggregatorDescriptorFactory[] {
                        new CountAggregatorDescriptorFactory(1), new IntSumAggregatorDescriptorFactory(1, 2),
                        new ConcatAggregatorDescriptorFactory(4, 3) }), outRecordDescriptor,
                new HashSpillableGroupingTableFactory(new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }), tableSize),
                true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        // Output operator

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(parseFileSplits(outSplits));

        AbstractSingleActivityOperatorDescriptor printer;

        if (outToFile)
            printer = new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, "\t");
        else
            printer = new PrinterOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

        // Connections
        // Connection 1: from data generator to external grouper
        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, generator, 0, grouper, 0);

        // Connection 2: from external grouper to outputer
        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}
