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

import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.dataflow.std.connectors.globalagg.LocalGroupConnectorGenerator.GrouperConnector;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;

/**
 * A logical representation of a global aggregation plan
 */
public class GlobalAggregationPlan {

    private final String[] nodes, inputNodes, dataFilePaths;

    private LocalGroupOperatorDescriptor.GroupAlgorithms localGrouperAlgo;
    private String[] localGrouperPartition;

    private GrouperConnector localConnector;
    private BitSet localConnNodeMap;

    private List<LocalGroupOperatorDescriptor.GroupAlgorithms> globalGrouperAlgos;
    private List<String[]> globalGroupersPartitions;

    private List<GrouperConnector> globalConnectors;
    private List<BitSet> globalConnNodeMaps;

    private CostVector costVector;

    private DatasetStats outputStat;

    public GlobalAggregationPlan(String[] nodes, String[] inputNodes, String[] dataFilePaths) {
        this.nodes = nodes;
        this.inputNodes = inputNodes;
        this.dataFilePaths = dataFilePaths;
        this.globalGrouperAlgos = new LinkedList<>();
        this.globalGroupersPartitions = new LinkedList<>();
        this.globalConnNodeMaps = new LinkedList<>();
        this.costVector = new CostVector();
        this.outputStat = new DatasetStats();
        this.globalConnectors = new LinkedList<>();
    }

    public GlobalAggregationPlan createCopy() {
        GlobalAggregationPlan copy = new GlobalAggregationPlan(nodes, inputNodes, dataFilePaths);

        // copy local grouper
        copy.localGrouperAlgo = localGrouperAlgo;
        copy.localGrouperPartition = new String[localGrouperPartition.length];
        for (int i = 0; i < copy.localGrouperPartition.length; i++) {
            copy.localGrouperPartition[i] = localGrouperPartition[i];
        }

        // copy local connector
        copy.localConnector = this.localConnector;
        copy.localConnNodeMap = (BitSet) this.localConnNodeMap.clone();

        // copy global groupers
        for (int i = 0; i < globalGrouperAlgos.size(); i++) {
            copy.globalGrouperAlgos.add(globalGrouperAlgos.get(i));
        }
        for (int i = 0; i < globalGroupersPartitions.size(); i++) {
            copy.globalGroupersPartitions.add(Arrays.copyOfRange(globalGroupersPartitions.get(i), 0,
                    globalGroupersPartitions.get(i).length));
        }

        // copy global connectors
        for (int i = 0; i < globalConnNodeMaps.size(); i++) {
            copy.globalConnNodeMaps.add((BitSet) globalConnNodeMaps.get(i).clone());
        }
        for (int i = 0; i < globalConnectors.size(); i++) {
            copy.globalConnectors.add(globalConnectors.get(i));
        }

        copy.outputStat.setGroupCount(this.outputStat.getGroupCount());
        copy.outputStat.setRecordCount(this.outputStat.getRecordCount());
        copy.outputStat.setRecordSize(this.outputStat.getRecordSize());

        copy.costVector.setCpu(costVector.getCpu());
        copy.costVector.setIo(costVector.getIo());
        copy.costVector.setNetwork(costVector.getNetwork());

        return copy;
    }

    public String toString() {
        StringBuilder planStringBuilder = new StringBuilder();

        boolean isFirst = true;

        planStringBuilder.append("Plan:").append(costVector.toString()).append("\nInputs:");

        // print input nodes
        for (int i = 0; i < inputNodes.length; i++) {
            planStringBuilder.append("    ").append(inputNodes[i]);
        }
        planStringBuilder.append("\nLocalConn: ").append(localConnector.name()).append("(\n");

        // print local connector node map
        printNodeMap(planStringBuilder, inputNodes.length, inputNodes.length, localConnNodeMap);

        // print local grouper
        planStringBuilder.append("\nLocalGrouper: ").append(localGrouperAlgo.name()).append("(");
        // - print local grouper partition constraint
        isFirst = true;
        for (String inputNode : inputNodes) {
            if (isFirst) {
                isFirst = false;
            } else {
                planStringBuilder.append(", ");
            }
            planStringBuilder.append(inputNode);
        }
        planStringBuilder.append(")");

        String[] prevPartitionConstraint = localGrouperPartition;

        // print global groupers
        for (int i = 0; i < globalGrouperAlgos.size(); i++) {
            planStringBuilder.append("\nGlobalConn").append(i).append(": ").append(globalConnectors.get(i).name())
                    .append("(\n");

            // print global connector node map
            printNodeMap(planStringBuilder, prevPartitionConstraint.length, globalGroupersPartitions.get(i).length,
                    globalConnNodeMaps.get(i));

            planStringBuilder.append(")");

            // print global grouper
            planStringBuilder.append("\nGlobalGrouper").append(i).append(": ").append(globalGrouperAlgos.get(i).name())
                    .append("(");
            // - print global grouper partition constraint
            isFirst = true;
            for (String inputNode : globalGroupersPartitions.get(i)) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    planStringBuilder.append(", ");
                }
                planStringBuilder.append(inputNode);
            }
            planStringBuilder.append(")");

            prevPartitionConstraint = globalGroupersPartitions.get(i);
        }

        return planStringBuilder.toString();
    }

    private void printNodeMap(StringBuilder sbder, int fromNodes, int toNodes, BitSet nodeMap) {
        for (int i = 0; i < fromNodes; i++) {
            for (int j = 0; j < toNodes; j++) {
                if (nodeMap.get(i * toNodes + j)) {
                    sbder.append('*');
                } else {
                    sbder.append('o');
                }
            }
            if (i < fromNodes - 1) {
                sbder.append("\n");
            }
        }
    }

    public CostVector getCostVector() {
        return costVector;
    }

    public DatasetStats getOutputStat() {
        return outputStat;
    }

    public LocalGroupOperatorDescriptor.GroupAlgorithms getLocalGrouperAlgo() {
        return localGrouperAlgo;
    }

    public void setLocalGrouperAlgo(LocalGroupOperatorDescriptor.GroupAlgorithms localGrouperAlgo) {
        this.localGrouperAlgo = localGrouperAlgo;
    }

    public String[] getLocalGrouperPartition() {
        return localGrouperPartition;
    }

    public void setLocalGrouperPartition(String[] localPartition) {
        this.localGrouperPartition = localPartition;
    }

    public BitSet getLocalConnNodeMap() {
        return localConnNodeMap;
    }

    public void setLocalConnNodeMap(BitSet localPartitionMap) {
        this.localConnNodeMap = localPartitionMap;
    }

    public List<LocalGroupOperatorDescriptor.GroupAlgorithms> getGlobalGrouperAlgos() {
        return globalGrouperAlgos;
    }

    public void setGlobalGrouperAlgos(List<LocalGroupOperatorDescriptor.GroupAlgorithms> globalGrouperAlgos) {
        this.globalGrouperAlgos = globalGrouperAlgos;
    }

    public List<String[]> getGlobalGroupersPartitions() {
        return globalGroupersPartitions;
    }

    public void setGlobalGroupersPartitions(List<String[]> globalPartitions) {
        this.globalGroupersPartitions = globalPartitions;
    }

    public List<BitSet> getGlobalConnNodeMaps() {
        return globalConnNodeMaps;
    }

    public void setGlobalConnNodeMaps(List<BitSet> partitionMaps) {
        this.globalConnNodeMaps = partitionMaps;
    }

    public GrouperConnector getLocalConnector() {
        return localConnector;
    }

    public void setLocalConnector(GrouperConnector localConnector) {
        this.localConnector = localConnector;
    }

    public List<GrouperConnector> getGlobalConnectors() {
        return globalConnectors;
    }

    public void setGlobalConnectors(List<GrouperConnector> globalConnectors) {
        this.globalConnectors = globalConnectors;
    }

    public void setCostVector(CostVector costVector) {
        this.costVector = costVector;
    }

    public void setOutputStat(DatasetStats outputStat) {
        this.outputStat = outputStat;
    }

    public String[] getNodes() {
        return nodes;
    }

    public String[] getInputNodes() {
        return inputNodes;
    }

    public String[] getDataFilePaths() {
        return dataFilePaths;
    }
    
    

}
