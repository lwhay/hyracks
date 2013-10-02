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
import java.util.Arrays;
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
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashFunctionFamilyFactoryAdapter;

public class LocalGroupCostDescriptor {

    public static JobSpecification createHyracksJobSpec(int framesLimit, int[] keyFields, int[] decorFields,
            long inputCount, long outputCount, int groupStateSize, double fudgeFactor, int tableSize,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, ITupleParserFactory parserFactory,
            IAggregatorDescriptorFactory aggregateFactory, IAggregatorDescriptorFactory partialMergeFactory,
            IAggregatorDescriptorFactory finalMergeFactory, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFamilies, INormalizedKeyComputerFactory firstNormalizerFactory,
            GlobalAggregationPlan aggPlan) throws IOException, HyracksDataException {

        JobSpecification spec = new JobSpecification();

        int[] storedKeyFields = new int[keyFields.length];
        int[] storedDecorFields = new int[decorFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        for (int i = storedKeyFields.length; i < storedKeyFields.length + storedDecorFields.length; i++) {
            storedDecorFields[i] = i;
        }

        FileSplit[] inputSplits = new FileSplit[aggPlan.getDataFilePaths().length];

        for (int i = 0; i < inputSplits.length; i++) {
            inputSplits[i] = new FileSplit(aggPlan.getInputNodes()[i], new FileReference(new File(
                    aggPlan.getDataFilePaths()[i])));
        }

        StringBuilder outputLabel = new StringBuilder();

        IFileSplitProvider inputSplitProvider = new ConstantFileSplitProvider(inputSplits);
        FileScanOperatorDescriptor inputScanner = new FileScanOperatorDescriptor(spec, inputSplitProvider,
                parserFactory, inRecDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, inputScanner, aggPlan.getInputNodes());

        IOperatorDescriptor prevGrouper;

        int[] keys, decors;
        IAggregatorDescriptorFactory inputAggFactory;

        LocalGroupOperatorDescriptor localGrouper = new LocalGroupOperatorDescriptor(spec, keyFields, decorFields,
                framesLimit, tableSize, inputCount, outputCount, groupStateSize, fudgeFactor, comparatorFactories,
                hashFamilies, firstNormalizerFactory, aggregateFactory, partialMergeFactory, finalMergeFactory,
                outRecDesc, aggPlan.getLocalGrouperAlgo(), 0);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, localGrouper, aggPlan.getLocalGrouperPartition());

        IConnectorDescriptor localConn = new LocalityAwareMToNPartitioningConnectorDescriptor(
                spec,
                new FieldHashPartitionComputerFactory(
                        keyFields,
                        new IBinaryHashFunctionFactory[] { HashFunctionFamilyFactoryAdapter
                                .getFunctionFactoryFromFunctionFamily(MurmurHash3BinaryHashFunctionFamily.INSTANCE, 0) }),
                new HashtableLocalityMap(aggPlan.getLocalConnNodeMap()));
        spec.connect(localConn, inputScanner, 0, localGrouper, 0);

        outputLabel.append('_').append(aggPlan.getLocalGrouperAlgo().name());

        prevGrouper = localGrouper;
        keys = storedKeyFields;
        decors = storedDecorFields;
        inputAggFactory = partialMergeFactory;

        String[] prevPartitions = aggPlan.getInputNodes();

        for (int i = 0; i < aggPlan.getGlobalGrouperAlgos().size(); i++) {
            LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, keys, decors, framesLimit,
                    tableSize, inputCount, outputCount, groupStateSize, fudgeFactor, comparatorFactories, hashFamilies,
                    firstNormalizerFactory, inputAggFactory, partialMergeFactory, finalMergeFactory, outRecDesc,
                    aggPlan.getGlobalGrouperAlgos().get(i), i + 1);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, aggPlan
                    .getGlobalGroupersPartitions().get(i));

            IConnectorDescriptor conn = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields,
                            new IBinaryHashFunctionFactory[] { HashFunctionFamilyFactoryAdapter
                                    .getFunctionFactoryFromFunctionFamily(MurmurHash3BinaryHashFunctionFamily.INSTANCE,
                                            i) }), new HashtableLocalityMap(aggPlan.getGlobalConnNodeMaps().get(i)));

            spec.connect(conn, prevGrouper, 0, grouper, 0);

            outputLabel.append('_').append(aggPlan.getGlobalGrouperAlgos().get(i).name());

            prevGrouper = grouper;
            keys = storedKeyFields;
            decors = storedDecorFields;
            inputAggFactory = partialMergeFactory;
            prevPartitions = aggPlan.getGlobalGroupersPartitions().get(i);
        }

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, prevPartitions,
                "global" + outputLabel.toString());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, prevPartitions);

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

    public static Map<GrouperProperty, List<GlobalAggregationPlan>> exploreForNonDominatedGlobalAggregationPlans(
            int framesLimit, int frameSize, long inputCount, long outputCount, int groupStateSize, double fudgeFactor,
            int tableSize, String[] nodes, String[] inputNodes, String[] dataFilePaths, double htCapRatio,
            int htSlotSize, int htRefSize, double bfErrorRatio) throws HyracksDataException {

        // used to track the usage of nodes
        boolean[] usedNodes = new boolean[nodes.length];
        for (int i = 0; i < inputNodes.length; i++) {
            for (int j = 0; j < nodes.length; j++) {
                if (nodes[j].equalsIgnoreCase(inputNodes[i])) {
                    usedNodes[j] = true;
                    break;
                }
            }
        }

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
                conn.computeCostVector(currentCostVector, currentDataStat, frameSize, nodeMap);
                GrouperProperty currentOutputProperty = currentDataProperty.createCopy();
                conn.computeOutputProperty(nodeMap, currentOutputProperty);

                for (GroupAlgorithms algo : GroupAlgorithms.values()) {
                    if (!currentOutputProperty.isCompatibleWithMask(algo.getRequiredProperty())) {
                        continue;
                    }
                    GlobalAggregationPlan plan = new GlobalAggregationPlan(nodes, inputNodes, dataFilePaths);

                    // set the local connector
                    plan.setLocalConnector(conn);
                    // set the node map for local connector
                    plan.setLocalConnNodeMap((BitSet) nodeMap.getNodeMap().clone());

                    // set the local grouper
                    plan.setLocalGrouperAlgo(algo);
                    // set the local grouper partition constraint
                    plan.setLocalGrouperPartition(inputNodes);

                    // added cost from the local connector
                    plan.getCostVector().updateWithCostVector(currentCostVector);
                    // reset the dataset stats with the output of the connector
                    plan.getOutputStat().resetWithDatasetStats(currentDataStat);
                    // update the cost vector
                    algo.computeCostVector(plan.getCostVector(), plan.getOutputStat(), framesLimit, frameSize,
                            tableSize, fudgeFactor, htCapRatio, htSlotSize, htRefSize, bfErrorRatio);
                    // compute the output property, which will be used as the key for this subplan in the map
                    GrouperProperty planOutputProperty = currentOutputProperty.createCopy();
                    algo.computeOutputProperty(planOutputProperty);

                    if (localSubplans.get(planOutputProperty) == null) {
                        localSubplans.put(planOutputProperty, new ArrayList<GlobalAggregationPlan>());
                    }
                    localSubplans.get(planOutputProperty).add(plan);
                }
            }
        }
        allSubPlans.put(inputNodes.length, localSubplans);

        // start to compute the subplans recursively, by firstly setting the final aggregation nodes.
        for (int finalGrouperNodes = 1; finalGrouperNodes <= nodes.length - inputNodes.length; finalGrouperNodes++) {
            int subplanNodes = nodes.length - finalGrouperNodes;

            boolean[] usedNodesCopy = Arrays.copyOfRange(usedNodes, 0, usedNodes.length);
            String[] grouperPartitionConstraint = getGrouperPartitionConstraint(finalGrouperNodes, nodes, usedNodesCopy);

            exploreSubplans(allSubPlans, subplanNodes, framesLimit, frameSize, inputCount, outputCount, groupStateSize,
                    fudgeFactor, tableSize, htCapRatio, htSlotSize, htRefSize, bfErrorRatio, nodes, inputNodes,
                    usedNodesCopy);
            Map<GrouperProperty, List<GlobalAggregationPlan>> subplanSets = allSubPlans.get(subplanNodes);
            if (subplanSets == null || subplanSets.size() == 0) {
                continue;
            }
            // for each output property, generate the possible plan
            for (GrouperProperty subplanProp : subplanSets.keySet()) {
                // firstly, add the proper connector
                for (GrouperConnector newConn : GrouperConnector.values()) {
                    // check whether the connector can handle the data outputted from the subplan
                    if (!subplanProp.isCompatibleWithMask(newConn.getRequiredProperty())) {
                        continue;
                    }
                    for (GlobalAggregationPlan subplan : subplanSets.get(subplanProp)) {
                        String[] subplanFinalGrouperPartitionConstraint;
                        if (subplan.getGlobalGrouperAlgos().size() > 0) {
                            subplanFinalGrouperPartitionConstraint = subplan.getGlobalGroupersPartitions().get(
                                    subplan.getGlobalGrouperAlgos().size() - 1);
                        } else {
                            subplanFinalGrouperPartitionConstraint = subplan.getLocalGrouperPartition();
                        }
                        List<PartitionNodeMap> validConnNodeMaps = getValidNodeMaps(
                                subplanFinalGrouperPartitionConstraint.length, finalGrouperNodes);
                        for (PartitionNodeMap connNodeMap : validConnNodeMaps) {
                            GrouperProperty propertyForConnectorOutput = subplanProp.createCopy();
                            newConn.computeOutputProperty(connNodeMap, propertyForConnectorOutput);
                            // check all possible terminal groupers
                            for (GroupAlgorithms terminal : GroupAlgorithms.values()) {
                                // check whether the grouper can be a terminal
                                if (!terminal.canBeTerminal()) {
                                    continue;
                                }
                                // check whether  the grouper can handle the data outputted from the connector
                                if (!propertyForConnectorOutput.isCompatibleWithMask(terminal.getRequiredProperty())) {
                                    continue;
                                }

                                // create a new plan by copying the old plan
                                GlobalAggregationPlan subplanCopy = subplan.createCopy();

                                // add a new connector
                                subplanCopy.getGlobalConnectors().add(newConn);
                                subplanCopy.getGlobalConnNodeMaps().add((BitSet) connNodeMap.getNodeMap().clone());

                                // update the cost after adding the new connector 
                                // TODO similar to the update constraint function in algebrics?
                                newConn.computeCostVector(subplanCopy.getCostVector(), subplanCopy.getOutputStat(),
                                        frameSize, connNodeMap);

                                // add a new grouper
                                subplanCopy.getGlobalGrouperAlgos().add(terminal);
                                subplanCopy.getGlobalGroupersPartitions().add(grouperPartitionConstraint);

                                // update the cost after adding the new grouper
                                terminal.computeCostVector(subplanCopy.getCostVector(), subplanCopy.getOutputStat(),
                                        framesLimit, frameSize, tableSize, fudgeFactor, htCapRatio, htSlotSize,
                                        htRefSize, bfErrorRatio);

                                // get the output property
                                GrouperProperty subplanOutputProp = propertyForConnectorOutput.createCopy();
                                terminal.computeOutputProperty(subplanOutputProp);

                                if (!subplanOutputProp.isAggregationDone()) {
                                    continue;
                                }

                                // add the new plan into the subplan map
                                Map<GrouperProperty, List<GlobalAggregationPlan>> planSets = allSubPlans
                                        .get(nodes.length);
                                if (planSets == null) {
                                    planSets = new HashMap<>();
                                    allSubPlans.put(nodes.length, planSets);
                                }
                                List<GlobalAggregationPlan> plansForProp = planSets.get(subplanOutputProp);
                                if (plansForProp == null) {
                                    plansForProp = new LinkedList<>();
                                    planSets.put(subplanOutputProp, plansForProp);
                                }
                                plansForProp.add(subplanCopy);
                            }
                        }
                    }
                }
            }
        }

        return allSubPlans.get(nodes.length);
    }

    private static String[] getGrouperPartitionConstraint(int nodes, String[] allNodes, boolean[] usedNodes) {
        String[] partConstraint = new String[nodes];
        int j = 0;
        for (int i = 0; i < allNodes.length; i++) {
            if (usedNodes[i]) {
                continue;
            }
            partConstraint[j] = allNodes[i];
            usedNodes[i] = true;
            j++;
            if (j >= partConstraint.length) {
                break;
            }
        }
        return partConstraint;
    }

    private static void exploreSubplans(Map<Integer, Map<GrouperProperty, List<GlobalAggregationPlan>>> allSubPlans,
            int nodesToUse, int framesLimit, int frameSize, long inputCount, long outputCount, int groupStateSize,
            double fudgeFactor, int tableSize, double htCapRatio, int htSlotSize, int htRefSize, double bfErrorRatio,
            String[] nodes, String[] inputNodes, boolean[] usedNodes) throws HyracksDataException {
        if (allSubPlans.containsKey(nodesToUse)) {
            // if the subplans for subplanNodes are already being explored, no further search is needed
            return;
        }

        // start to compute the subplans recursively, by firstly setting the final aggregation nodes.
        for (int finalGrouperNodes = 1; finalGrouperNodes <= nodesToUse - inputNodes.length; finalGrouperNodes++) {
            int subplanNodes = nodesToUse - finalGrouperNodes;

            boolean[] usedNodesCopy = Arrays.copyOfRange(usedNodes, 0, usedNodes.length);
            String[] grouperPartitionConstraint = getGrouperPartitionConstraint(finalGrouperNodes, nodes, usedNodesCopy);

            exploreSubplans(allSubPlans, subplanNodes, framesLimit, frameSize, inputCount, outputCount, groupStateSize,
                    fudgeFactor, tableSize, htCapRatio, htSlotSize, htRefSize, bfErrorRatio, nodes, inputNodes,
                    usedNodesCopy);
            Map<GrouperProperty, List<GlobalAggregationPlan>> subplanSets = allSubPlans.get(subplanNodes);
            if (subplanSets == null || subplanSets.size() == 0) {
                continue;
            }
            // for each output property, generate the possible plan
            for (GrouperProperty subplanProp : subplanSets.keySet()) {
                // firstly, add the proper connector
                for (GrouperConnector newConn : GrouperConnector.values()) {
                    // check whether the connector can handle the data outputted from the subplan
                    if (!subplanProp.isCompatibleWithMask(newConn.getRequiredProperty())) {
                        continue;
                    }
                    for (GlobalAggregationPlan subplan : subplanSets.get(subplanProp)) {
                        String[] subplanFinalGrouperPartitionConstraint;
                        if (subplan.getGlobalGrouperAlgos().size() > 0) {
                            subplanFinalGrouperPartitionConstraint = subplan.getGlobalGroupersPartitions().get(
                                    subplan.getGlobalGrouperAlgos().size() - 1);
                        } else {
                            subplanFinalGrouperPartitionConstraint = subplan.getLocalGrouperPartition();
                        }
                        List<PartitionNodeMap> validConnNodeMaps = getValidNodeMaps(
                                subplanFinalGrouperPartitionConstraint.length, finalGrouperNodes);
                        for (PartitionNodeMap connNodeMap : validConnNodeMaps) {
                            GrouperProperty propertyForConnectorOutput = subplanProp.createCopy();
                            newConn.computeOutputProperty(connNodeMap, propertyForConnectorOutput);
                            // check all possible terminal groupers
                            for (GroupAlgorithms terminal : GroupAlgorithms.values()) {

                                // check whether  the grouper can handle the data outputted from the connector
                                if (!propertyForConnectorOutput.isCompatibleWithMask(terminal.getRequiredProperty())) {
                                    continue;
                                }

                                // get the output property
                                GrouperProperty subplanOutputProp = propertyForConnectorOutput.createCopy();
                                terminal.computeOutputProperty(subplanOutputProp);

                                if (subplanOutputProp.isAggregationDone()) {
                                    continue;
                                }

                                // create a new plan by copying the old plan
                                GlobalAggregationPlan subplanCopy = subplan.createCopy();

                                // add a new connector
                                subplanCopy.getGlobalConnectors().add(newConn);
                                subplanCopy.getGlobalConnNodeMaps().add((BitSet) connNodeMap.getNodeMap().clone());

                                // update the cost after adding the new connector 
                                // TODO similar to the update constraint function in algebrics?
                                newConn.computeCostVector(subplanCopy.getCostVector(), subplanCopy.getOutputStat(),
                                        frameSize, connNodeMap);

                                // add a new grouper
                                subplanCopy.getGlobalGrouperAlgos().add(terminal);
                                subplanCopy.getGlobalGroupersPartitions().add(grouperPartitionConstraint);

                                // update the cost after adding the new grouper
                                terminal.computeCostVector(subplanCopy.getCostVector(), subplanCopy.getOutputStat(),
                                        framesLimit, frameSize, tableSize, fudgeFactor, htCapRatio, htSlotSize,
                                        htRefSize, bfErrorRatio);

                                // add the new plan into the subplan map
                                Map<GrouperProperty, List<GlobalAggregationPlan>> planSets = allSubPlans
                                        .get(nodesToUse);
                                if (planSets == null) {
                                    planSets = new HashMap<>();
                                    allSubPlans.put(nodesToUse, planSets);
                                }
                                List<GlobalAggregationPlan> plansForProp = planSets.get(subplanOutputProp);
                                if (plansForProp == null) {
                                    plansForProp = new LinkedList<>();
                                    planSets.put(subplanOutputProp, plansForProp);
                                }
                                plansForProp.add(subplanCopy);
                            }
                        }
                    }
                }
            }
        }
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
