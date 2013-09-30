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

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;

public class GlobalAggregationPlan {
    
    private LocalGroupOperatorDescriptor.GroupAlgorithms localGrouperAlgo;
    private String[] localPartition;
    private BitSet localPartitionMap;

    private List<LocalGroupOperatorDescriptor.GroupAlgorithms> globalGrouperAlgos;
    private List<String[]> globalPartitions;
    private List<BitSet> partitionMaps;

    private CostVector costVector;

    private DatasetStats outputStat;

    public GlobalAggregationPlan() {
        this.globalGrouperAlgos = new LinkedList<>();
        this.globalPartitions = new LinkedList<>();
        this.partitionMaps = new LinkedList<>();
        this.costVector = new CostVector();
        this.outputStat = new DatasetStats();
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

    public String[] getLocalPartition() {
        return localPartition;
    }

    public void setLocalPartition(String[] localPartition) {
        this.localPartition = localPartition;
    }

    public BitSet getLocalPartitionMap() {
        return localPartitionMap;
    }

    public void setLocalPartitionMap(BitSet localPartitionMap) {
        this.localPartitionMap = localPartitionMap;
    }

    public List<LocalGroupOperatorDescriptor.GroupAlgorithms> getGlobalGrouperAlgos() {
        return globalGrouperAlgos;
    }

    public void setGlobalGrouperAlgos(List<LocalGroupOperatorDescriptor.GroupAlgorithms> globalGrouperAlgos) {
        this.globalGrouperAlgos = globalGrouperAlgos;
    }

    public List<String[]> getGlobalPartitions() {
        return globalPartitions;
    }

    public void setGlobalPartitions(List<String[]> globalPartitions) {
        this.globalPartitions = globalPartitions;
    }

    public List<BitSet> getPartitionMaps() {
        return partitionMaps;
    }

    public void setPartitionMaps(List<BitSet> partitionMaps) {
        this.partitionMaps = partitionMaps;
    }

}
