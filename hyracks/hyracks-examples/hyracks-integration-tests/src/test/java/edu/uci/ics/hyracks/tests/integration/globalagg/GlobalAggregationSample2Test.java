/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.tests.integration.globalagg;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.globalagg.LocalGroupConnectorGenerator.GrouperConnector;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.DoubleSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPv6MarkStringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.GlobalAggregationPlan;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.LocalGroupCostDescriptor;

public class GlobalAggregationSample2Test extends AbstractGlobalAggIntegrationTest {

    final String DATA_FOLDER = "/Volumes/Home/Datasets/AggBench/global/small_sample";

    final String DATA_LABEL = "z0_1000000000_1000000000";

    final String DATA_SUFFIX = ".dat.small.part.";

    final int IPMARSK = 4;

    final String[] inputNodeIDs = new String[4];
    final String[] filePaths = new String[4];

    {
        // initialize the input partitions
        for (int i = 0; i < filePaths.length; i++) {
            filePaths[i] = DATA_FOLDER + "/" + DATA_LABEL + DATA_SUFFIX + i;
            inputNodeIDs[i] = NC_IDS[i];
        }
    }

    final RecordDescriptor inputRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
            // IP
            UTF8StringSerializerDeserializer.INSTANCE,
            // ad revenue
            DoubleSerializerDeserializer.INSTANCE });

    final RecordDescriptor outputRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
            // IP
            UTF8StringSerializerDeserializer.INSTANCE,
            // ad revenue
            DoubleSerializerDeserializer.INSTANCE });

    final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
            IPv6MarkStringParserFactory.getInstance(IPMARSK), DoubleParserFactory.INSTANCE }, '|');

    final IAggregatorDescriptorFactory aggregateFactory = new MultiFieldsAggregatorFactory(
            new IFieldAggregateDescriptorFactory[] { new DoubleSumFieldAggregatorFactory(1, false) });

    final IAggregatorDescriptorFactory partialMergeFactory = aggregateFactory;
    final IAggregatorDescriptorFactory finalMergeFactory = aggregateFactory;

    final IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY) };

    final IBinaryHashFunctionFamily[] hashFamilies = new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE };

    final INormalizedKeyComputerFactory firstNormalizerFactory = new UTF8StringNormalizedKeyComputerFactory();

    int framesLimit = 64;
    int tableSize = 8171;

    int[] keyFields = new int[] { 0 };
    int[] decorFields = new int[] {};

    int inputCount = 600571;
    int outputCount = 100000;
    int groupStateInBytes = 64;
    double fudgeFactor = 1.4;

    LocalGroupOperatorDescriptor.GroupAlgorithms localGrouperAlgo = LocalGroupOperatorDescriptor.GroupAlgorithms.HASH_GROUP;
    String[] localPartition = new String[] { NC_IDS[0], NC_IDS[1], NC_IDS[2], NC_IDS[3] };

    GrouperConnector localConn = GrouperConnector.HASH_CONN;
    BitSet localConnNodeMap = new BitSet(8);

    {
        localConnNodeMap.set(0);
        localConnNodeMap.set(2);
        localConnNodeMap.set(5);
        localConnNodeMap.set(7);
    }

    List<LocalGroupOperatorDescriptor.GroupAlgorithms> grouperAlgos = new LinkedList<>();
    List<String[]> partitionConstraints = new LinkedList<>();

    {
        grouperAlgos.add(LocalGroupOperatorDescriptor.GroupAlgorithms.SIMPLE_HYBRID_HASH);
        grouperAlgos.add(LocalGroupOperatorDescriptor.GroupAlgorithms.SORT_GROUP_MERGE_GROUP);
        partitionConstraints.add(new String[] { NC_IDS[4], NC_IDS[5] });
        partitionConstraints.add(new String[] { NC_IDS[6], NC_IDS[7] });
    }

    List<BitSet> globalConnNodeMaps = new LinkedList<>();
    List<GrouperConnector> globalConnectors = new LinkedList<>();

    {

        // initialize the partition maps 
        BitSet globalPartitionConstraint0 = new BitSet(4 * 2);
        globalPartitionConstraint0.set(0);
        globalPartitionConstraint0.set(2);
        globalPartitionConstraint0.set(5);
        globalPartitionConstraint0.set(7);

        globalConnNodeMaps.add(globalPartitionConstraint0);
        globalConnectors.add(GrouperConnector.HASH_CONN);

        BitSet globalPartitionConstraint1 = new BitSet(2 * 2);
        globalPartitionConstraint1.set(0, 4);

        globalConnNodeMaps.add(globalPartitionConstraint1);
        globalConnectors.add(GrouperConnector.HASH_CONN);
    }

    @Test
    public void twoLevelsTest() throws Exception {

        GlobalAggregationPlan plan = new GlobalAggregationPlan(NC_IDS, inputNodeIDs, filePaths);

        plan.setLocalGrouperAlgo(localGrouperAlgo);
        plan.setLocalGrouperPartition(inputNodeIDs);

        plan.setLocalConnector(GrouperConnector.HASH_CONN);
        BitSet localConnNodeMap = new BitSet(16);
        localConnNodeMap.set(0, 16);
        plan.setLocalConnNodeMap(localConnNodeMap);

        plan.setGlobalGrouperAlgos(grouperAlgos);
        plan.setGlobalGroupersPartitions(partitionConstraints);

        plan.setGlobalConnectors(globalConnectors);
        plan.setGlobalConnNodeMaps(globalConnNodeMaps);

        JobSpecification spec = LocalGroupCostDescriptor.createHyracksJobSpec(framesLimit, keyFields, decorFields,
                inputCount, outputCount, groupStateInBytes, fudgeFactor, tableSize, inputRecordDescriptor,
                outputRecordDescriptor, tupleParserFactory, aggregateFactory, partialMergeFactory, finalMergeFactory,
                comparatorFactories, hashFamilies, firstNormalizerFactory, plan);
        runTest(spec);

    }
}
