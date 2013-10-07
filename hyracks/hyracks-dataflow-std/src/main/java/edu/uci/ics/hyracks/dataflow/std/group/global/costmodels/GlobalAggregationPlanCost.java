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

import java.util.LinkedList;
import java.util.List;

/**
 * The cost for a global aggregation plan
 */
public class GlobalAggregationPlanCost {

    private final long inputCount, outputCount;
    private final int groupStateSize;

    private CostVector localConnCostVector;

    private CostVector localCostVector;
    private DatasetStats localGrouperOutputStats;

    private List<CostVector> globalConnCostVectors;

    public CostVector getLocalConnCostVector() {
        return localConnCostVector;
    }

    public void setLocalConnCostVector(CostVector localConnCostVector) {
        this.localConnCostVector = localConnCostVector;
    }

    public List<CostVector> getGlobalConnCostVectors() {
        return globalConnCostVectors;
    }

    public void setGlobalConnCostVectors(List<CostVector> globalConnCostVectors) {
        this.globalConnCostVectors = globalConnCostVectors;
    }

    private List<CostVector> globalCostVectors;
    private List<DatasetStats> globalGroupersOutputStats;

    public GlobalAggregationPlanCost(long inputCount, long outputCount, int groupStateSize) {
        this.inputCount = inputCount;
        this.outputCount = outputCount;
        this.groupStateSize = groupStateSize;
        this.globalConnCostVectors = new LinkedList<>();
        this.globalCostVectors = new LinkedList<>();
        this.globalGroupersOutputStats = new LinkedList<>();
    }

    public CostVector getLocalCostVector() {
        return localCostVector;
    }

    public void setLocalCostVector(CostVector localCostVector) {
        this.localCostVector = localCostVector;
    }

    public DatasetStats getLocalGrouperOutputStats() {
        return localGrouperOutputStats;
    }

    public void setLocalGrouperOutputStats(DatasetStats localGrouperOutputStats) {
        this.localGrouperOutputStats = localGrouperOutputStats;
    }

    public List<CostVector> getGlobalCostVectors() {
        return globalCostVectors;
    }

    public void setGlobalCostVectors(List<CostVector> globalCostVectors) {
        this.globalCostVectors = globalCostVectors;
    }

    public List<DatasetStats> getGlobalGroupersOutputStats() {
        return globalGroupersOutputStats;
    }

    public void setGlobalGroupersOutputStats(List<DatasetStats> globalGroupersOutputStats) {
        this.globalGroupersOutputStats = globalGroupersOutputStats;
    }

    public long getInputCount() {
        return inputCount;
    }

    public long getOutputCount() {
        return outputCount;
    }

    public int getGroupStateSize() {
        return groupStateSize;
    }

    public String toString() {
        StringBuilder sbder = new StringBuilder();

        sbder.append("PlanCost:\n");

        sbder.append("LOCAL[").append(localCostVector.toString()).append("]");

        return sbder.toString();
    }

}
