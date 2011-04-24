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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DataGeneratorOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyDataProcessingOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInOutOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInputOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyInputSinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.DummyOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.IGenDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.ITypeGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.IntegerGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.RandomDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.UTF8StringGenerator;
import edu.uci.ics.hyracks.dataflow.std.benchmarking.ZipfDistributionDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNRangePartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

/**
 * @author jarodwen
 */
public class DummiesWorkflowTests extends AbstractIntegrationTest {

    final int chainLength = 5;
    final int connectorType = 0;
    final int dataSize = 60000;
    final double cardRatio = 0.25;
    final int[] hashKeys = new int[] { 0 };
    final static boolean isOutputFile = true;
    final int randSeed = 38473;

    static final String outSplitsPrefix = System.getProperty("java.io.tmpdir");

    static final String outSplits1 = "nc1:" + outSplitsPrefix + "DummiesWorkflowTests_";
    static final String outSplits2 = "nc2:" + outSplitsPrefix + "DummiesWorkflowTests_";

    @Test
    public void dummyOperatorTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Dummy operator

        DummyOperatorDescriptor dummy = new DummyOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummy, NC2_ID, NC1_ID);

        spec.addRoot(dummy);

        runTest(spec);
    }

    @Test
    public void dummyInOutOperatorTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Dummy operator

        DummyInputOperatorDescriptor dummyIn = new DummyInputOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyIn, NC2_ID, NC1_ID);

        DummyInputSinkOperatorDescriptor dummySink = new DummyInputSinkOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummySink, NC2_ID, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn, dummyIn, 0, dummySink, 0);

        spec.addRoot(dummySink);

        runTest(spec);
    }

    @Test
    public void dummyInChainOutOperatorTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Dummy operator

        DummyInputOperatorDescriptor dummyIn = new DummyInputOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyIn, NC2_ID, NC1_ID);

        AbstractSingleActivityOperatorDescriptor opter = dummyIn;

        for (int i = 0; i < chainLength - 2; i++) {
            DummyInOutOperatorDescriptor dummyChain = new DummyInOutOperatorDescriptor(spec);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyChain, NC2_ID, NC1_ID);
            IConnectorDescriptor connChain;
            switch (connectorType) {
                case 0:
                    connChain = new OneToOneConnectorDescriptor(spec);
                    break;
                case 1:
                    connChain = new MToNHashPartitioningConnectorDescriptor(spec,
                            new FieldHashPartitionComputerFactory(new int[] {}, new IBinaryHashFunctionFactory[] {}));
                    break;
                case 2:
                    connChain = new MToNRangePartitioningConnectorDescriptor(spec, 0, null);
                    break;
                case 3:
                    connChain = new MToNHashPartitioningMergingConnectorDescriptor(spec,
                            new FieldHashPartitionComputerFactory(new int[] {}, new IBinaryHashFunctionFactory[] {}),
                            new int[] {}, new IBinaryComparatorFactory[] {});
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

        DummyInputSinkOperatorDescriptor dummySink = new DummyInputSinkOperatorDescriptor(spec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummySink, NC2_ID, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn, opter, 0, dummySink, 0);

        spec.addRoot(dummySink);

        runTest(spec);
    }

    @Test
    public void dummyDataPassingTest() throws Exception {

        JobSpecification spec = new JobSpecification();

        // Data Generator Operator
        @SuppressWarnings("rawtypes")
        ITypeGenerator[] dataTypeGenerators = new ITypeGenerator[] { new UTF8StringGenerator(10, true, randSeed),
                new IntegerGenerator(97, 100000, randSeed), new UTF8StringGenerator(20, false, randSeed), new IntegerGenerator(randSeed),
                new UTF8StringGenerator(3, false, randSeed) };

        IGenDistributionDescriptor[] dataDistributionDescriptors = new IGenDistributionDescriptor[] {
                new RandomDistributionDescriptor((int) (dataSize * cardRatio)),
                new RandomDistributionDescriptor((int) (dataSize * cardRatio)),
                new ZipfDistributionDescriptor((int) (dataSize * cardRatio), 1),
                new ZipfDistributionDescriptor((int) (dataSize * cardRatio), 0.5),
                new RandomDistributionDescriptor() };

        RecordDescriptor inRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        DataGeneratorOperatorDescriptor generator = new DataGeneratorOperatorDescriptor(spec, dataTypeGenerators,
                dataDistributionDescriptors, dataSize, true, randSeed);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, generator, NC2_ID, NC1_ID);

        IBinaryHashFunctionFactory[] hashFactories = new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE };
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE };

        AbstractSingleActivityOperatorDescriptor opter = generator;

        for (int i = 0; i < chainLength - 2; i++) {

            DummyDataProcessingOperatorDescriptor dummyChain = new DummyDataProcessingOperatorDescriptor(spec,
                    inRecordDescriptor);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dummyChain, NC2_ID, NC1_ID);

            IConnectorDescriptor connChain;
            switch (connectorType) {
                case 0:
                    connChain = new OneToOneConnectorDescriptor(spec);
                    break;
                case 1:
                    connChain = new MToNHashPartitioningConnectorDescriptor(spec,
                            new FieldHashPartitionComputerFactory(hashKeys, hashFactories));
                    break;
                // Currently RangePartition is not supported.
                //                case 2:
                //                    connChain = new MToNRangePartitioningConnectorDescriptor(spec, 0, null);
                //                    break;
                case 3:
                    connChain = new MToNHashPartitioningMergingConnectorDescriptor(spec,
                            new FieldHashPartitionComputerFactory(hashKeys, hashFactories), hashKeys,
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

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "dummyDataPassingTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, opter, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    private static AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, String prefix) {
        AbstractSingleActivityOperatorDescriptor printer;

        if (!isOutputFile)
            printer = new PrinterOperatorDescriptor(spec);
        else
            printer = new PlainFileWriterOperatorDescriptor(spec, new ConstantFileSplitProvider(
                    parseFileSplits(outSplits1 + prefix + ".nc1, " + outSplits2 + prefix + ".nc2")), "\t");

        return printer;
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
}
