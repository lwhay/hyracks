package edu.uci.ics.hyracks.tests.integration;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.ByteBasedBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.AvgFieldGroupAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.AvgFieldMergeAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.FloatSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MinMaxStringFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.OriginalHybridHashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;

public class OriginalHybridHashGroupTest extends AbstractIntegrationTest {
    final IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC2_ID,
            new FileReference(new File("data/tpch0.001/lineitem.tbl"))) });

    final int userProvidedInputSizeOfRawRecords = 6000000;//571;
    final int userProvidedInputSizeOfUniqueRecords = 1750000;//000;
    final boolean doInputAdjustment = true;
    final boolean doPartitionTune = true;

    final int framesLimit = 6;
    final int tableSize = 4096;

    final int[] testAlgorithms = new int[] { 0, 1, 2 };

    final double fudgeFactor = 1.2;

    final RecordDescriptor desc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

    final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
            UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
            IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
            FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, }, '|');

    private AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, String prefix)
            throws IOException {

        AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID, createTempFile()
                        .getAbsolutePath()) }), "\t");

        return printer;
    }

    @Test
    public void singleKeySumHybridHashSortTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE });
        int estimatedRecSize = 10 + 4 + 4 + 4 + 4 * 4 + 4;

        int[] keyFields = new int[] { 0 };

        OriginalHybridHashGroupOperatorDescriptor grouper = new OriginalHybridHashGroupOperatorDescriptor(spec,
                keyFields, framesLimit, userProvidedInputSizeOfRawRecords, userProvidedInputSizeOfUniqueRecords,
                tableSize, estimatedRecSize, fudgeFactor,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new FieldHashPartitionComputerFamily(keyFields,
                        new IBinaryHashFunctionFamily[] { ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new FieldHashPartitionComputerFamily(keyFields,
                        new IBinaryHashFunctionFamily[] { ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                new IntSumFieldAggregatorFactory(3, false),
                                new FloatSumFieldAggregatorFactory(5, false) }), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                new IntSumFieldAggregatorFactory(2, false),
                                new FloatSumFieldAggregatorFactory(3, false) }), outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        int[] storedKeys = new int[] { 0 };

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, storedKeys, null,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        IConnectorDescriptor conn2 = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(storedKeys,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }), storedKeys,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) });

        spec.connect(conn2, grouper, 0, sorter, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "singleKeySumHybridHashSortTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn3 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn3, sorter, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndCheckCorrectness(spec, new File[] { new File("data/tpch0.001/aggresults/singlekeysum.dat") },
                outputRec);
        //runTest(spec);
    }

    @Test
    public void singleKeyAvgHybridHashSortTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE });
        int estimatedRecSize = 10 + 4 + 4 + 4 + 4 * 4 + 4;

        int[] keyFields = new int[] { 0 };

        OriginalHybridHashGroupOperatorDescriptor grouper = new OriginalHybridHashGroupOperatorDescriptor(spec,
                keyFields, framesLimit, userProvidedInputSizeOfRawRecords, userProvidedInputSizeOfUniqueRecords,
                tableSize, estimatedRecSize, fudgeFactor,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new FieldHashPartitionComputerFamily(keyFields,
                        new IBinaryHashFunctionFamily[] { ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new FieldHashPartitionComputerFamily(keyFields,
                        new IBinaryHashFunctionFamily[] { ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new UTF8StringNormalizedKeyComputerFactory(),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(1, false), new CountFieldAggregatorFactory(false),
                        new AvgFieldGroupAggregatorFactory(1, false) }), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                new IntSumFieldAggregatorFactory(2, false),
                                new AvgFieldMergeAggregatorFactory(3, false) }), outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        int[] storedKeys = new int[] { 0 };

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, storedKeys, null,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        IConnectorDescriptor conn2 = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(storedKeys,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }), storedKeys,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                true);

        spec.connect(conn2, grouper, 0, sorter, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "singleKeyAvgHybridHashSortTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn3 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn3, sorter, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndCheckCorrectness(spec, new File[] { new File("data/tpch0.001/aggresults/singlekeyavg.dat") },
                outputRec);
    }

    @Test
    public void singleKeyMinMaxStringHybridHashSortTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });
        int estimatedRecSize = 10 + 4 + 44 + 4 * 3 + 4;

        int[] keyFields = new int[] { 0 };

        OriginalHybridHashGroupOperatorDescriptor grouper = new OriginalHybridHashGroupOperatorDescriptor(spec,
                keyFields, framesLimit, userProvidedInputSizeOfRawRecords, userProvidedInputSizeOfUniqueRecords,
                tableSize, estimatedRecSize, fudgeFactor,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new FieldHashPartitionComputerFamily(keyFields,
                        new IBinaryHashFunctionFamily[] { ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new FieldHashPartitionComputerFamily(keyFields,
                        new IBinaryHashFunctionFamily[] { ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                new MinMaxStringFieldAggregatorFactory(15, true, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(1, false),
                        new MinMaxStringFieldAggregatorFactory(2, true, true) }), outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        int[] storedKeys = new int[] { 0 };

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, storedKeys, null,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        IConnectorDescriptor conn2 = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(storedKeys,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }), storedKeys,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                true);

        spec.connect(conn2, grouper, 0, sorter, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "singleKeyMinMaxStringHybridHashSortTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn3 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn3, sorter, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndCheckCorrectness(spec, new File[] { new File("data/tpch0.001/aggresults/singlekeymax.dat") },
                outputRec);
    }

    @Test
    public void multiKeySumHybridHashSortTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        int estimatedRecSize = 10 + 10 + 4 + 4 + 4 * 4 + 4;

        int[] keyFields = new int[] { 8, 0 };
        int[] storedKeyFields = new int[] { 0, 1 };

        OriginalHybridHashGroupOperatorDescriptor grouper = new OriginalHybridHashGroupOperatorDescriptor(spec,
                keyFields, framesLimit, userProvidedInputSizeOfRawRecords, userProvidedInputSizeOfUniqueRecords,
                tableSize, estimatedRecSize, fudgeFactor, new IBinaryComparatorFactory[] {
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new FieldHashPartitionComputerFamily(keyFields, new IBinaryHashFunctionFamily[] {
                        ByteBasedBinaryHashFunctionFamily.INSTANCE, ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new FieldHashPartitionComputerFamily(storedKeyFields, new IBinaryHashFunctionFamily[] {
                        ByteBasedBinaryHashFunctionFamily.INSTANCE, ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                new IntSumFieldAggregatorFactory(3, false) }), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(2, false),
                                new IntSumFieldAggregatorFactory(3, false) }), outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        int[] storedKeys = new int[] { 0, 1 };

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, storedKeys, null,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        IConnectorDescriptor conn2 = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(storedKeys, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }), storedKeys,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, true);

        spec.connect(conn2, grouper, 0, sorter, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "multiKeySumHybridHashSortTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn3 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn3, sorter, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndCheckCorrectness(spec, new File[] { new File("data/tpch0.001/aggresults/multikeysum.dat") },
                outputRec);
    }

    @Test
    public void multiKeyAvgHybridHashSortTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE });
        int estimatedRecSize = 10 + 10 + 4 + 4 + 4 + 4 * 5 + 4;

        int[] keyFields = new int[] { 8, 0 };
        int[] storedKeyFields = new int[] { 0, 1 };

        OriginalHybridHashGroupOperatorDescriptor grouper = new OriginalHybridHashGroupOperatorDescriptor(spec,
                keyFields, framesLimit, userProvidedInputSizeOfRawRecords, userProvidedInputSizeOfUniqueRecords,
                tableSize, estimatedRecSize, fudgeFactor, new IBinaryComparatorFactory[] {
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new FieldHashPartitionComputerFamily(keyFields, new IBinaryHashFunctionFamily[] {
                        ByteBasedBinaryHashFunctionFamily.INSTANCE, ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new FieldHashPartitionComputerFamily(storedKeyFields, new IBinaryHashFunctionFamily[] {
                        ByteBasedBinaryHashFunctionFamily.INSTANCE, ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new UTF8StringNormalizedKeyComputerFactory(),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(1, false), new CountFieldAggregatorFactory(false),
                        new AvgFieldGroupAggregatorFactory(1, false) }), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(2, false),
                                new IntSumFieldAggregatorFactory(3, false),
                                new AvgFieldMergeAggregatorFactory(4, false) }), outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        int[] storedKeys = new int[] { 0, 1 };

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, storedKeys, null,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        IConnectorDescriptor conn2 = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(storedKeys, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }), storedKeys,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, true);

        spec.connect(conn2, grouper, 0, sorter, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "multiKeySumHybridHashSortTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn3 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn3, sorter, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndCheckCorrectness(spec, new File[] { new File("data/tpch0.001/aggresults/multikeyavg.dat") },
                outputRec);
    }

    @Test
    public void multiKeyMinMaxStringHybridHashSortTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });
        int estimatedRecSize = 10 + 10 + 4 + 44 + 4 * 4 + 4;

        int[] keyFields = new int[] { 8, 0 };
        int[] storedKeyFields = new int[] { 0, 1 };

        OriginalHybridHashGroupOperatorDescriptor grouper = new OriginalHybridHashGroupOperatorDescriptor(spec,
                keyFields, framesLimit, userProvidedInputSizeOfRawRecords, userProvidedInputSizeOfUniqueRecords,
                tableSize, estimatedRecSize, fudgeFactor, new IBinaryComparatorFactory[] {
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new FieldHashPartitionComputerFamily(keyFields, new IBinaryHashFunctionFamily[] {
                        ByteBasedBinaryHashFunctionFamily.INSTANCE, ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new FieldHashPartitionComputerFamily(storedKeyFields, new IBinaryHashFunctionFamily[] {
                        ByteBasedBinaryHashFunctionFamily.INSTANCE, ByteBasedBinaryHashFunctionFamily.INSTANCE }),
                new UTF8StringNormalizedKeyComputerFactory(), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                                new MinMaxStringFieldAggregatorFactory(15, true, true) }),
                new MultiFieldsAggregatorFactory(new int[] { 0, 1 }, new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(2, false),
                        new MinMaxStringFieldAggregatorFactory(3, true, true) }), outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        int[] storedKeys = new int[] { 0, 1 };

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, storedKeys, null,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, outputRec);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        IConnectorDescriptor conn2 = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(storedKeys, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }), storedKeys,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, true);

        spec.connect(conn2, grouper, 0, sorter, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "multiKeySumHybridHashSortTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn3 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn3, sorter, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndCheckCorrectness(spec, new File[] { new File("data/tpch0.001/aggresults/multikeymax.dat") },
                outputRec);
    }
}
