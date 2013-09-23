/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.tests.integration.globalagg;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.FloatSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MinMaxStringFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.global.aggregators.AvgFieldAggregateAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.aggregators.AvgFieldFinalMergeAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.aggregators.AvgFieldPartialMergeAggregatorFactory;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class GlobalLocalGroupAggregationTest extends AbstractIntegrationTest {

    final IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC2_ID,
            new FileReference(new File("/Volumes/Home/Datasets/tpch/tpch0.1/lineitem.tbl"))) });

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

    int framesLimit = 1024;
    int tableSize = 8171;

    int inputCount = 600571;
    int groupStateInBytes = 64;
    double fudgeFactor = 1.4;

    LocalGroupOperatorDescriptor.GroupAlgorithms algo = LocalGroupOperatorDescriptor.GroupAlgorithms.RECURSIVE_HYBRID_HASH;

    private AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, String prefix)
            throws IOException {

        ResultSetId rsId = new ResultSetId(1);
        AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                new ConstantFileSplitProvider(new FileSplit[] {
                        new FileSplit(NC1_ID, new FileReference(new File("/Volumes/Home/hyracks_tmp/" + prefix
                                + "_nc1.log"))),
                        new FileSplit(NC2_ID, new FileReference(new File("/Volumes/Home/hyracks_tmp/" + prefix
                                + "_nc2.log"))) }), "|");
        spec.addResultSetId(rsId);

        return printer;
    }

    /**
     * <pre>
     * select 
     * count(*), 
     * sum(L_PARTKEY), sum(L_LINENUMBER), sum(L_EXTENDEDPRICE), 
     * avg(L_PARTKEY), 
     * max(length(L_COMMENT)), min(length(L_COMMENT)) 
     * from LINEITEM;
     * </pre>
     * 
     * which should return
     * 
     * <pre>
     * 6005, 615388, 17990, 152774398.38, 102.479267277269, 43, 10
     * </pre>
     * 
     * Note that there is no string-min or string-max function in SQL, so the SQL command here uses the min/max
     * of the string length.
     * 
     * @throws Exception
     */
    @Test
    public void noKeySumGlobalGroupTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] {};
        int groupCount = 1;

        LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, keyFields, new int[] {},
                framesLimit, tableSize, inputCount, groupCount, groupStateInBytes, fudgeFactor,
                new IBinaryComparatorFactory[] {}, new IBinaryHashFunctionFamily[] {}, null,
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new CountFieldAggregatorFactory(false), new IntSumFieldAggregatorFactory(1, false),
                        new IntSumFieldAggregatorFactory(3, false), new FloatSumFieldAggregatorFactory(5, false),
                        new AvgFieldAggregateAggregatorFactory(1, false),
                        new MinMaxStringFieldAggregatorFactory(15, true, true),
                        new MinMaxStringFieldAggregatorFactory(15, false, true) }), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] {
                                new IntSumFieldAggregatorFactory(keyFields.length, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                                new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                                new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldFinalMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec, algo,
                0);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID);

        IConnectorDescriptor conn1 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "local_" + algo.name() + "_nokey");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void singleKeySumGlobalGroupTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 1 };
        int groupCount = 20000;

        LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, keyFields, new int[] {},
                framesLimit, tableSize, inputCount, groupCount, groupStateInBytes, fudgeFactor,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, null,
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new CountFieldAggregatorFactory(false), new IntSumFieldAggregatorFactory(1, false),
                        new IntSumFieldAggregatorFactory(3, false), new FloatSumFieldAggregatorFactory(5, false),
                        new AvgFieldAggregateAggregatorFactory(1, false),
                        new MinMaxStringFieldAggregatorFactory(15, true, true),
                        new MinMaxStringFieldAggregatorFactory(15, false, true) }), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] {
                                new IntSumFieldAggregatorFactory(keyFields.length, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                                new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                                new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldFinalMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec, algo,
                0);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID);

        IConnectorDescriptor conn1 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "local_" + algo.name() + "_singlekey");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void multiKeySumGlobalGroupTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 8, 1 };
        int groupCount = 59975;

        LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, keyFields, new int[] {},
                framesLimit, tableSize, inputCount, groupCount, groupStateInBytes, fudgeFactor,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE,
                        MurmurHash3BinaryHashFunctionFamily.INSTANCE }, null, new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false),
                                new IntSumFieldAggregatorFactory(1, false), new IntSumFieldAggregatorFactory(3, false),
                                new FloatSumFieldAggregatorFactory(5, false),
                                new AvgFieldAggregateAggregatorFactory(1, false),
                                new MinMaxStringFieldAggregatorFactory(15, true, true),
                                new MinMaxStringFieldAggregatorFactory(15, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldFinalMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec, algo,
                0);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID);

        IConnectorDescriptor conn1 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "local_" + algo.name() + "_multikey");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}
