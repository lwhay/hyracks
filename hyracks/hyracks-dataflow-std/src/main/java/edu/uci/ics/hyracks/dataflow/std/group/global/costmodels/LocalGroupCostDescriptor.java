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
package edu.uci.ics.hyracks.dataflow.std.group.global.costmodels;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.HashtableLocalityMap;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.PartitionNodeMap;
import edu.uci.ics.hyracks.dataflow.std.connectors.globalagg.LocalGroupConnectorGenerator.GrouperConnector;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor.GroupAlgorithms;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.GrouperProperty.Property;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashFunctionFamilyFactoryAdapter;

public class LocalGroupCostDescriptor {

    public static JobSpecification createHyracksJobSpec(int framesLimit, int[] keyFields, int[] decorFields,
            long inputCount, long outputCount, int groupStateSize, double fudgeFactor, int tableSize, String[] nodes,
            String[] inputNodes, FileSplit[] inputSplits, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            ITupleParserFactory parserFactory, IAggregatorDescriptorFactory aggregateFactory,
            IAggregatorDescriptorFactory partialMergeFactory, IAggregatorDescriptorFactory finalMergeFactory,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFamily[] hashFamilies,
            INormalizedKeyComputerFactory firstNormalizerFactory,
            LocalGroupOperatorDescriptor.GroupAlgorithms localGrouperAlgo,
            LocalGroupOperatorDescriptor.GroupAlgorithms[] globalGrouperAlgos, String[] localPartition,
            String[][] globalPartitions, BitSet[] partitionMaps) throws IOException, HyracksDataException {

        JobSpecification spec = new JobSpecification();

        int[] storedKeyFields = new int[keyFields.length];
        int[] storedDecorFields = new int[decorFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        for (int i = storedKeyFields.length; i < storedKeyFields.length + storedDecorFields.length; i++) {
            storedDecorFields[i] = i;
        }

        StringBuilder outputLabel = new StringBuilder();

        IFileSplitProvider inputSplitProvider = new ConstantFileSplitProvider(inputSplits);
        FileScanOperatorDescriptor inputScanner = new FileScanOperatorDescriptor(spec, inputSplitProvider,
                parserFactory, inRecDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, inputScanner, inputNodes);

        IOperatorDescriptor prevGrouper;

        int[] keys, decors;
        IAggregatorDescriptorFactory inputAggFactory;

        if (localGrouperAlgo == GroupAlgorithms.NO_OPERATION) {
            prevGrouper = inputScanner;
            keys = keyFields;
            decors = decorFields;
            inputAggFactory = aggregateFactory;
        } else {
            LocalGroupOperatorDescriptor localGrouper = new LocalGroupOperatorDescriptor(spec, keyFields, decorFields,
                    framesLimit, tableSize, inputCount, outputCount, groupStateSize, fudgeFactor, comparatorFactories,
                    hashFamilies, firstNormalizerFactory, aggregateFactory, partialMergeFactory, finalMergeFactory,
                    outRecDesc, localGrouperAlgo, 0);

            IConnectorDescriptor localConn = new OneToOneConnectorDescriptor(spec);
            spec.connect(localConn, inputScanner, 0, localGrouper, 0);

            outputLabel.append('_').append(localGrouperAlgo.name());

            prevGrouper = localGrouper;
            keys = storedKeyFields;
            decors = storedDecorFields;
            inputAggFactory = partialMergeFactory;
        }

        for (int i = 0; i < globalGrouperAlgos.length; i++) {
            LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, keys, decors, framesLimit,
                    tableSize, inputCount, outputCount, groupStateSize, fudgeFactor, comparatorFactories, hashFamilies,
                    firstNormalizerFactory, inputAggFactory, partialMergeFactory, finalMergeFactory, outRecDesc,
                    globalGrouperAlgos[i], i + 1);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, globalPartitions[i]);

            IConnectorDescriptor conn = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields,
                            new IBinaryHashFunctionFactory[] { HashFunctionFamilyFactoryAdapter
                                    .getFunctionFactoryFromFunctionFamily(MurmurHash3BinaryHashFunctionFamily.INSTANCE,
                                            i) }), new HashtableLocalityMap(partitionMaps[i]));

            spec.connect(conn, prevGrouper, 0, grouper, 0);

            outputLabel.append('_').append(globalGrouperAlgos[i].name());

            prevGrouper = grouper;
            keys = storedKeyFields;
            decors = storedDecorFields;
            inputAggFactory = partialMergeFactory;
        }

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
                globalPartitions[globalPartitions.length - 1], "global" + outputLabel.toString());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
                globalPartitions[globalPartitions.length - 1]);

        IConnectorDescriptor printConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(printConn, prevGrouper, 0, printer, 0);

        spec.addRoot(printer);

        return spec;
    }

    private static AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, String[] outputNodeIDs,
            String prefix) throws IOException {

        FileSplit[] outputSplits = new FileSplit[outputNodeIDs.length];
        for (int i = 0; i < outputNodeIDs.length; i++) {
            outputSplits[i] = new FileSplit(outputNodeIDs[i], new FileReference(new File("/Volumes/Home/hyracks_tmp/"
                    + prefix + "_" + outputNodeIDs[i] + ".log")));
        }

        ResultSetId rsId = new ResultSetId(1);
        AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                new ConstantFileSplitProvider(outputSplits), "|");
        spec.addResultSetId(rsId);

        return printer;
    }

    public static List<GlobalAggregationPlan> exploreForNonDominatedGlobalAggregationPlans(int framesLimit,
            int frameSize, long inputCount, long outputCount, int groupStateSize, double fudgeFactor, int tableSize,
            String[] nodes, String[] inputNodes, double htCapRatio, int htSlotSize, int htRefSize, double bfErrorRatio)
            throws HyracksDataException {
        List<GlobalAggregationPlan> plans = new LinkedList<GlobalAggregationPlan>();

        Map<Integer, Map<GrouperProperty, List<GlobalAggregationPlan>>> allSubPlans = new HashMap<>();

        // add the plan for local grouper
        Map<GrouperProperty, List<GlobalAggregationPlan>> localSubplans = new HashMap<>();

        // get the input data statistics
        DatasetStats currentDataStat = new DatasetStats(inputCount, outputCount, groupStateSize);

        // get all valid node maps
        List<PartitionNodeMap> validLocalNodeMaps = getValidNodeMaps(inputNodes.length, inputNodes.length);
        // add the one-to-one node map, as the valid node map generator skips it.
        PartitionNodeMap localOneToOneNodeMap = new PartitionNodeMap(inputNodes.length, inputNodes.length);
        GrouperProperty currentDataProperty = new GrouperProperty();
        localOneToOneNodeMap.setAsOneToOneNodeMap();
        validLocalNodeMaps.add(localOneToOneNodeMap);

        for (GrouperConnector conn : GrouperConnector.values()) {
            if (!currentDataProperty.isCompatibleWithMask(conn.getRequiredProperty())) {
                continue;
            }

            for (PartitionNodeMap nodeMap : validLocalNodeMaps) {
                CostVector currentCostVector = new CostVector();
                conn.computeCostVector(currentCostVector, currentDataStat, groupStateSize, frameSize, nodeMap);
                GrouperProperty currentOutputProperty = currentDataProperty.createCopy();
                conn.computeOutputProperty(nodeMap, currentOutputProperty);

                for (GroupAlgorithms algo : GroupAlgorithms.values()) {
                    if (!currentOutputProperty.isCompatibleWithMask(algo.getRequiredProperty())) {
                        continue;
                    }
                    GlobalAggregationPlan plan = new GlobalAggregationPlan();
                    plan.setLocalGrouperAlgo(algo);
                    plan.setLocalPartition(inputNodes);
                    plan.setLocalPartitionMap((BitSet) nodeMap.getNodeMap().clone());
                    // added cost from the local connector
                    plan.getCostVector().updateWithCostVector(currentCostVector);
                    // reset the dataset stats with the output of the connector
                    plan.getOutputStat().resetWithDatasetStats(currentDataStat);
                    // update the cost vector
                    algo.computeCostVector(plan.getCostVector(), plan.getOutputStat(), framesLimit, frameSize,
                            tableSize, fudgeFactor, htCapRatio, htSlotSize, htRefSize, bfErrorRatio);
                    // compute the output property, which will be used as the key for this subplan in the map
                    GrouperProperty planOutputProperty = currentOutputProperty.createCopy();
                    algo.computeOutputProperty(currentOutputProperty);
                    
                    if (localSubplans.get(planOutputProperty) == null) {
                        localSubplans.put(planOutputProperty, new ArrayList<GlobalAggregationPlan>());
                    }
                    localSubplans.get(planOutputProperty).add(plan);
                }
            }
        }
        allSubPlans.put(inputNodes.length, localSubplans);

        // start to compute the subplans recursively.
        
        return plans;
    }

    private static void exploreSubplans(Map<Integer, Map<GrouperProperty, List<GlobalAggregationPlan>>> subplans,
            int subplanNodes, int framesLimit, long inputCount, long outputCount, int groupStateSize,
            double fudgeFactor, int tableSize, String[] nodes, String[] inputNodes) throws HyracksDataException {
        if (subplans.containsKey(subplanNodes)) {
            // if the subplans for subplanNodes are already being explored, no further search is needed
            return;
        }
        Map<GrouperProperty, List<GlobalAggregationPlan>> foundSubplans = new HashMap<GrouperProperty, List<GlobalAggregationPlan>>();

        subplans.put(subplanNodes, foundSubplans);
    }

    private static List<PartitionNodeMap> getValidNodeMaps(int fromNodes, int toNodes) throws HyracksDataException {
        List<PartitionNodeMap> validNodeMaps = new ArrayList<>();

        for (int fanOut = 1; fanOut <= toNodes; fanOut++) {
            if (fanOut * fromNodes % toNodes == 0) {
                int fanIn = fanOut * fromNodes / toNodes;
                if (fanIn == 1) {
                    continue;
                }
                if (fanIn <= fromNodes && fromNodes % fanIn == 0) {
                    PartitionNodeMap nodeMap = new PartitionNodeMap(fromNodes, toNodes);
                    setNodeMapConnections(fanIn, fanOut, nodeMap);
                    validNodeMaps.add(nodeMap);
                }
            }
        }

        return validNodeMaps;
    }

    private static void setNodeMapConnections(int fanIn, int fanOut, PartitionNodeMap nodeMap)
            throws HyracksDataException {
        nodeMap.reset();
        int[] fanOuts = new int[nodeMap.getFromNodes()];
        int[] fanIns = new int[nodeMap.getToNodes()];
        for (int fromIdx = 0; fromIdx < nodeMap.getFromNodes(); fromIdx++) {
            int toIdx = 0;
            while (fanOuts[fromIdx] < fanOut) {
                while (toIdx < nodeMap.getToNodes()) {
                    if (fanIns[toIdx] < fanIn) {
                        break;
                    } else {
                        toIdx++;
                    }
                }
                if (toIdx >= nodeMap.getToNodes() || fanIns[toIdx] >= fanIn) {
                    throw new HyracksDataException("Failed to find a toNode for the given fromNode: " + fromIdx);
                }
                nodeMap.setMap(fromIdx, toIdx);
                fanIns[toIdx]++;
                fanOuts[fromIdx]++;
                toIdx++;
            }
        }
    }
}
