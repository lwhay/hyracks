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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.RecursiveHybridHashGrouper;

public class CommonCompoents {

    /**
     * Get the non-empty hash table slots if inserting insertedKeys into a hash table with hashtableSlots slots
     * 
     * @param insertedKeys
     *            The number of unique keys to be inserted into the hash table
     * @param hashtableSlots
     *            The total number of slots in the hash table
     * @return
     */
    public static double getHashtableUsedSlots(double insertedKeys, double hashtableSlots) {
        return hashtableSlots * (1 - Math.pow(1 - 1 / hashtableSlots, insertedKeys));
    }

    /**
     * Compute the costs for a precluster grouper, assuming that the input data is already sorted.
     * 
     * @param costVector
     * @param rawRecordsCount
     * @param uniqueKeyCount
     * @param recordSizeInBytes
     */
    public static void preclusterCost(CostVector costVector, DatasetStats outputStat) {
        costVector.updateCpu(outputStat.getRecordCount());
        outputStat.setGroupCount((long) outputStat.getGroupCount());
        outputStat.setRecordCount((long) outputStat.getGroupCount());
    }

    public static void simpleHybridHashCost(CostVector costVector, DatasetStats outputStat, double memoryInPages,
            double pageSizeInBytes, double hashtableSlotCapacityRatio, double fudgeExtraFactor, double htSlotSize,
            double htRefSize, double bloomFilterErrorRatio) {

        double rawRecordsCount = outputStat.getRecordCount();
        double uniqueKeyCount = outputStat.getGroupCount();
        double recordSizeInBytes = outputStat.getRecordSize();

        if (uniqueKeyCount < 0 || rawRecordsCount < 0) {
            throw new IllegalStateException("Incorrect input parameter: uniqueKeyCount " + uniqueKeyCount
                    + ", rawRecordsCount " + rawRecordsCount);
        }

        outputStat.setRecordCount(0);

        double keysInEachPartition = uniqueKeyCount;
        double rawRecordsInEachPartition = rawRecordsCount;
        double partitionCount = 1;
        double fudgeFactor = computeFudgeFactor(recordSizeInBytes, memoryInPages, pageSizeInBytes,
                hashtableSlotCapacityRatio, fudgeExtraFactor, htSlotSize, htRefSize);
        double hashtableSlotCount = getHashtableSlotsCount(recordSizeInBytes, memoryInPages, pageSizeInBytes,
                hashtableSlotCapacityRatio, htSlotSize, htRefSize);

        if (uniqueKeyCount * recordSizeInBytes * fudgeFactor / pageSizeInBytes < memoryInPages - 1) {
            // record can be totally aggregated in memory, so only hash-group is needed
            costVector.updateCpu(hashHitCPUCost(rawRecordsCount, uniqueKeyCount, hashtableSlotCount));
            outputStat.updateRecordCount((long) uniqueKeyCount);
            return;
        }

        // start hybrid hash
        double hybridHashPartitionCount = 1;

        // compute the average non-empty slot length in the hash table
        double usedSlotsCount = getHashtableUsedSlots((memoryInPages - hybridHashPartitionCount) * pageSizeInBytes
                / fudgeExtraFactor / recordSizeInBytes, hashtableSlotCount);
        double avgUsedSlotLength = (memoryInPages - hybridHashPartitionCount) * pageSizeInBytes / fudgeExtraFactor
                / recordSizeInBytes / usedSlotsCount;

        // the portion of the resident partition among the over data
        double residentPartitionPortion = (memoryInPages - hybridHashPartitionCount) / fudgeFactor
                / (uniqueKeyCount * recordSizeInBytes / pageSizeInBytes);

        if (residentPartitionPortion < 0 || residentPartitionPortion > 1) {
            throw new IllegalAccessError("Incorrect resident partition portion ratio: " + residentPartitionPortion);
        }

        // the hash-hit cost is only from the resident partition: all records belonging to the resident partition
        // will be aggregated by a hash hit.
        costVector.updateCpu(partitionCount
                * hashHitCPUCost(rawRecordsInEachPartition * residentPartitionPortion, keysInEachPartition
                        * residentPartitionPortion, hashtableSlotCount));

        // the hash-miss cost
        costVector.updateCpu(partitionCount * (rawRecordsInEachPartition * (1 - residentPartitionPortion))
                * bloomFilterErrorRatio * avgUsedSlotLength);

        outputStat
                .updateRecordCount((long) (partitionCount * (keysInEachPartition * residentPartitionPortion + rawRecordsInEachPartition
                        * (1 - residentPartitionPortion))));
    }

    /**
     * The hybrid-hash cost computer.
     * 
     * @param costVector
     * @param rawRecordsCount
     * @param uniqueKeyCount
     * @param memoryInPages
     * @param pageSizeInBytes
     * @param recordSizeInBytes
     * @param hashtableSlotCapacityRatio
     */
    public static void recursiveHybridHashCostComputer(CostVector costVector, DatasetStats outputStat,
            double memoryInPages, double pageSizeInBytes, double hashtableSlotCapacityRatio, double fudgeExtraFactor,
            double htSlotSize, double htRefSize, double bloomFilterErrorRatio) {

        double rawRecordsCount = outputStat.getRecordCount();
        double uniqueKeyCount = outputStat.getGroupCount();
        double recordSizeInBytes = outputStat.getRecordSize();

        if (uniqueKeyCount < 0 || rawRecordsCount < 0) {
            throw new IllegalStateException("Incorrect input parameter: uniqueKeyCount " + uniqueKeyCount
                    + ", rawRecordsCount " + rawRecordsCount);
        }

        outputStat.setRecordCount(0);

        double keysInEachPartition = uniqueKeyCount;
        double rawRecordsInEachPartition = rawRecordsCount;
        double partitionCount = 1;
        double fudgeFactor = computeFudgeFactor(recordSizeInBytes, memoryInPages, pageSizeInBytes,
                hashtableSlotCapacityRatio, fudgeExtraFactor, htSlotSize, htRefSize);
        double hashtableSlotCount = getHashtableSlotsCount(recordSizeInBytes, memoryInPages, pageSizeInBytes,
                hashtableSlotCapacityRatio, htSlotSize, htRefSize);

        if (uniqueKeyCount * recordSizeInBytes * fudgeFactor / pageSizeInBytes < memoryInPages - 1) {
            // record can be totally aggregated in memory, so only hash-group is needed
            costVector.updateCpu(hashHitCPUCost(rawRecordsCount, uniqueKeyCount, hashtableSlotCount));
            outputStat.updateRecordCount((long) uniqueKeyCount);
            return;
        }

        while (keysInEachPartition * recordSizeInBytes / pageSizeInBytes * fudgeFactor > memoryInPages * memoryInPages) {
            // partition using all memory
            keysInEachPartition = keysInEachPartition / memoryInPages;
            rawRecordsInEachPartition = rawRecordsInEachPartition / memoryInPages;
            partitionCount = partitionCount * memoryInPages;
            // append I/O cost: dumping the raw records onto the disk. (2x for both write and later load)
            costVector.updateIo(2 * rawRecordsCount);
        }

        // start hybrid hash
        double hybridHashPartitionCount = RecursiveHybridHashGrouper.computeHybridHashPartitions((int) memoryInPages,
                (int) pageSizeInBytes, (long) keysInEachPartition, (int) recordSizeInBytes, 1, fudgeFactor);
        //Math.ceil((keysInEachPartition * recordSizeInBytes * fudgeFactor / pageSizeInBytes - memoryInPages)
        //        / (memoryInPages - 1));

        // compute the average non-empty slot length in the hash table
        double usedSlotsCount = getHashtableUsedSlots((memoryInPages - hybridHashPartitionCount) * pageSizeInBytes
                / fudgeExtraFactor / recordSizeInBytes, hashtableSlotCount);
        double avgUsedSlotLength = (memoryInPages - hybridHashPartitionCount) * pageSizeInBytes / fudgeExtraFactor
                / recordSizeInBytes / usedSlotsCount;

        // the portion of the resident partition among the over data
        double residentPartitionPortion = ((memoryInPages - hybridHashPartitionCount) / (memoryInPages
                * hybridHashPartitionCount + memoryInPages - hybridHashPartitionCount));

        if (residentPartitionPortion < 0 || residentPartitionPortion > 1) {
            throw new IllegalAccessError("Incorrect resident partition portion ratio: " + residentPartitionPortion);
        }

        // the hash-hit cost is only from the resident partition: all records belonging to the resident partition
        // will be aggregated by a hash hit.
        costVector.updateCpu(partitionCount
                * hashHitCPUCost(rawRecordsInEachPartition * residentPartitionPortion, keysInEachPartition
                        * residentPartitionPortion, hashtableSlotCount));

        // the hash-miss cost
        costVector.updateCpu(partitionCount * (rawRecordsInEachPartition * (1 - residentPartitionPortion))
                * bloomFilterErrorRatio * avgUsedSlotLength);

        outputStat
                .updateRecordCount((long) (partitionCount * (keysInEachPartition * residentPartitionPortion + rawRecordsInEachPartition
                        * (1 - residentPartitionPortion))));

        // the spilled partitions are loaded back 
        // add the cost for dumping the spilled partitions; x2 means that to dump and load
        costVector.updateIo(partitionCount * (rawRecordsInEachPartition * (1 - residentPartitionPortion))
                * recordSizeInBytes / pageSizeInBytes * 2);
        double keysInSpilledPartition = keysInEachPartition
                * (memoryInPages / (memoryInPages * hybridHashPartitionCount + memoryInPages - hybridHashPartitionCount));
        double rawRecordsInSpilledPartition = rawRecordsInEachPartition
                * (memoryInPages / (memoryInPages * hybridHashPartitionCount + memoryInPages - hybridHashPartitionCount));
        // estimate the hash-hit cost for hashing the spilled records in memory
        costVector.updateCpu(partitionCount
                * hashHitCPUCost(rawRecordsInSpilledPartition, keysInSpilledPartition, keysInSpilledPartition
                        * hashtableSlotCapacityRatio) * hybridHashPartitionCount);
    }

    /**
     * Compute the CPU cost caused by the hash-hit if inserting recordsInserted records (with keysInserted keys) into a
     * hash table with hashtableSlotsCount slots, assuming all records can be inserted into the hash table without
     * overflowing the table.
     * 
     * @param recordsInserted
     * @param keysInserted
     * @param hashtableSlotsCount
     * @return
     */
    public static double hashHitCPUCost(double recordsInserted, double keysInserted, double hashtableSlotsCount) {
        double usedSlots = getHashtableUsedSlots(keysInserted, hashtableSlotsCount);
        double avgNonEmptySlotLength = keysInserted / usedSlots;
        double cpuCost = recordsInserted * (1 + avgNonEmptySlotLength) / 2;
        if (cpuCost < 0) {
            throw new IllegalStateException("Unreasonable cpu cost for hash hit: " + cpuCost);
        }
        return cpuCost;
    }

    public static double getHashtableCapacity(double recordSizeInBytes, double memoryInPages, double pageSizeInBytes,
            double hashtableSlotCapacityRatio, double htSlotSize, double htRefSize) {
        double htCapacity = memoryInPages * pageSizeInBytes
                / (hashtableSlotCapacityRatio * htSlotSize + htRefSize + recordSizeInBytes);
        if (htCapacity < 0) {
            throw new IllegalStateException("Unreasonable hashtable capcity: " + htCapacity);
        }
        return htCapacity;
    }

    public static double getHashtableSlotsCount(double recordSizeInBytes, double memoryInPages, double pageSizeInBytes,
            double hashtableSlotCapacityRatio, double htSlotSize, double htRefSize) {
        return getHashtableCapacity(recordSizeInBytes, memoryInPages, pageSizeInBytes, hashtableSlotCapacityRatio,
                htSlotSize, htRefSize) * hashtableSlotCapacityRatio;
    }

    /**
     * Compute the merge-group cost.
     * 
     * @param costVector
     * @param recordsInserted
     * @param keysInserted
     * @param estimatedRecordSizeInByte
     * @param sortedChunksCount
     * @param ctx
     */
    public static void mergeGroupCostComputer(CostVector costVector, double recordsInserted, double keysInserted,
            double estimatedRecordSizeInByte, double sortedChunksCount, double memoryInPages, double pageSizeInBytes) {
        List<Double> sortedRuns = new ArrayList<Double>();
        List<Double> rawRecordsRepresentedBySortedRuns = new ArrayList<Double>();
        for (int i = 0; i < sortedChunksCount; i++) {
            sortedRuns.add(recordsInserted / sortedChunksCount);
            rawRecordsRepresentedBySortedRuns.add(recordsInserted / sortedChunksCount);
        }
        while (sortedRuns.size() > 0) {
            // do a full merge
            double rawRecordsRepresentedByMergedRun = 0.0;
            double recordsToMerge = 0.0;
            int i = 0;
            for (i = 0; i < memoryInPages && sortedRuns.size() > 0; i++) {
                rawRecordsRepresentedByMergedRun += rawRecordsRepresentedBySortedRuns.remove(0);
                recordsToMerge += sortedRuns.remove(0);
            }
            // update the merge CPU cost
            costVector.updateCpu(recordsToMerge * (Math.log(i) / Math.log(2)));
            // update the merge IO cost for flushing and loading the merged file

            double recordsAfterMerge = CommonCompoents.getKeysInRecords(rawRecordsRepresentedByMergedRun,
                    recordsInserted, keysInserted);
            costVector.updateIo(2 * (recordsAfterMerge * estimatedRecordSizeInByte / pageSizeInBytes));
            sortedRuns.add(recordsAfterMerge);
            sortedRuns.add(rawRecordsRepresentedByMergedRun);
        }
    }

    /**
     * Compute the merge-group cost.
     * 
     * @param costVector
     * @param recordsInserted
     * @param keysInserted
     * @param estimatedRecordSizeInByte
     * @param sortedRuns
     * @param rawRecordsRepresentedBySortedRuns
     * @param ctx
     */
    public static void mergeGroupCostComputer(CostVector costVector, double recordsInserted, double keysInserted,
            double estimatedRecordSizeInByte, List<Double> sortedRuns, List<Double> rawRecordsRepresentedBySortedRuns,
            double memoryInPages, double pageSizeInBytes) {

        while (sortedRuns.size() > 0) {
            // do a full merge
            double rawRecordsRepresentedByMergedRun = 0.0;
            double recordsToMerge = 0.0;
            int i = 0;
            for (i = 0; i < memoryInPages && sortedRuns.size() > 0; i++) {
                rawRecordsRepresentedByMergedRun += rawRecordsRepresentedBySortedRuns.remove(0);
                recordsToMerge += sortedRuns.remove(0);
            }
            // update the merge CPU cost
            costVector.updateCpu(recordsToMerge * Math.max((Math.log(i) / Math.log(2)), 1));
            // update the merge IO cost for flushing and loading the merged file

            double recordsAfterMerge = CommonCompoents.getKeysInRecords(rawRecordsRepresentedByMergedRun,
                    recordsInserted, keysInserted);
            costVector.updateIo(2 * (recordsAfterMerge * estimatedRecordSizeInByte / pageSizeInBytes));
            if (sortedRuns.size() > 0) {
                sortedRuns.add(recordsAfterMerge);
                rawRecordsRepresentedBySortedRuns.add(rawRecordsRepresentedByMergedRun);
            }
        }
    }

    /**
     * Compute the fudge factor. The fudge factor is computed as:<br/>
     * F = hash table overhead ratio (o) * extra overhead (f) <br/>
     * where o can be precisely computed based on the hash table structure and the input data, while f is just an
     * estimation.
     * 
     * @param recordSizeInBytes
     * @param memoryInPages
     * @param pageSizeInBytes
     * @param hashtableSlotCapacityRatio
     * @param fudgeExtraFactor
     * @param htSlotSize
     * @param htRefSize
     * @return
     */
    public static double computeFudgeFactor(double recordSizeInBytes, double memoryInPages, double pageSizeInBytes,
            double hashtableSlotCapacityRatio, double fudgeExtraFactor, double htSlotSize, double htRefSize) {
        double fudgeFactor = ((htSlotSize * hashtableSlotCapacityRatio + htRefSize + recordSizeInBytes) / recordSizeInBytes)
                * fudgeExtraFactor;
        if (fudgeFactor <= 1 || fudgeFactor >= 2) {
            throw new IllegalStateException("Unreasonable Fudge Factor: " + fudgeFactor);
        }
        return fudgeFactor;
    }

    /**
     * Compute the cost vector for HashGroup + MergeGroup operator.
     * 
     * @param costVector
     * @param rawRecordsCount
     * @param uniqueKeyCount
     * @param recordSizeInBytes
     * @param ctx
     */
    public static void hashGroupSortMergeGroupCostComputer(CostVector costVector, DatasetStats outputStat,
            double memoryInPages, double pageSizeInBytes, double hashtableSlotCapacityRatio, double htSlotSize,
            double htRefSize, boolean hasSortMergeGroup) {

        double rawRecordsCount = outputStat.getRecordCount();
        double uniqueKeyCount = outputStat.getGroupCount();
        double recordSizeInBytes = outputStat.getRecordSize();

        outputStat.setRecordCount(0);

        double hashtableCapacity = getHashtableCapacity(recordSizeInBytes, memoryInPages, pageSizeInBytes,
                hashtableSlotCapacityRatio, htRefSize, htRefSize);
        double rawRecordsToFillHashTable = getRecordsForKeys(hashtableCapacity, rawRecordsCount, uniqueKeyCount);
        double hashtableSlotsCount = getHashtableSlotsCount(recordSizeInBytes, memoryInPages, pageSizeInBytes,
                hashtableSlotCapacityRatio, htSlotSize, htRefSize);
        int memoryFilledUpCount = (int) Math.ceil(rawRecordsCount / rawRecordsToFillHashTable);
        if (memoryFilledUpCount <= 1) {
            // record can be totally aggregated in memory, so only hash-group is needed
            costVector.updateCpu(hashHitCPUCost(rawRecordsCount, uniqueKeyCount, hashtableSlotsCount));

            outputStat.updateRecordCount((long) uniqueKeyCount);

            return;
        }
        List<Double> sortedRuns = new ArrayList<Double>();
        List<Double> rawRecordsRepresentedBySortedRuns = new ArrayList<Double>();
        // for the first (memoryFilledUpCount - 1) parts, the hash table is full
        for (int i = 0; i < memoryFilledUpCount - 1; i++) {
            sortedRuns.add(hashtableCapacity);
            rawRecordsRepresentedBySortedRuns.add(rawRecordsToFillHashTable);
        }

        // hash cost
        costVector.updateCpu((memoryFilledUpCount - 1)
                * hashHitCPUCost(rawRecordsToFillHashTable, hashtableCapacity, hashtableSlotsCount));

        if (hasSortMergeGroup) {
            // sort cost, if there is the sort-merge-group phases
            double usedSlots = getHashtableUsedSlots(hashtableCapacity, hashtableSlotsCount);
            double averageUsedSlotLength = hashtableCapacity / usedSlots;
            costVector.updateCpu((memoryFilledUpCount - 1) * usedSlots
                    * sortCPUCostComputer(averageUsedSlotLength, averageUsedSlotLength));
            // updated the I/O for flushing and loading (so it is 2x) the spilled data
            costVector
                    .updateIo(2 * (memoryFilledUpCount - 1) * hashtableCapacity * recordSizeInBytes / pageSizeInBytes);
        }

        outputStat.updateRecordCount((long) ((memoryFilledUpCount - 1) * hashtableCapacity));

        // for the last part
        double rawRecordsForLastPart = rawRecordsCount - (memoryFilledUpCount - 1) * rawRecordsToFillHashTable;
        double keysInLastPart = getKeysInRecords(rawRecordsForLastPart, rawRecordsCount, uniqueKeyCount);
        sortedRuns.add(keysInLastPart);
        rawRecordsRepresentedBySortedRuns.add(rawRecordsForLastPart);
        costVector.updateCpu(hashHitCPUCost(rawRecordsForLastPart, keysInLastPart, hashtableSlotsCount));

        if (hasSortMergeGroup) {
            double lastPartUsedSlots = getHashtableUsedSlots(keysInLastPart, hashtableSlotsCount);
            double lastPartAvgUsedSlotLength = keysInLastPart / lastPartUsedSlots;
            costVector.updateCpu(lastPartUsedSlots
                    * sortCPUCostComputer(lastPartAvgUsedSlotLength, lastPartAvgUsedSlotLength));

            costVector.updateIo(2 * keysInLastPart * recordSizeInBytes / pageSizeInBytes);
        }

        outputStat.updateRecordCount((long) keysInLastPart);

        // do merge-group
        if (hasSortMergeGroup) {
            mergeGroupCostComputer(costVector, rawRecordsCount, uniqueKeyCount, recordSizeInBytes, sortedRuns,
                    rawRecordsRepresentedBySortedRuns, memoryInPages, pageSizeInBytes);
        }
    }

    public static void sortGroupMergeGroupCostComputer(CostVector costVector, DatasetStats outputStat,
            double memoryInPages, double pageSizeInBytes, boolean hasMergeGroup) {

        double rawRecordsCount = outputStat.getRecordCount();
        double uniqueKeyCount = outputStat.getGroupCount();
        double recordSizeInBytes = outputStat.getRecordSize();

        outputStat.setRecordCount(0);

        double recordsInFullMemory = (memoryInPages - 1) * pageSizeInBytes / recordSizeInBytes;
        double keysInFullMemory = getKeysInRecords(recordsInFullMemory, rawRecordsCount, uniqueKeyCount);
        int sortedRunsCount = (int) (Math.ceil(rawRecordsCount / recordsInFullMemory));
        if (sortedRunsCount <= 1) {
            // all records can be sorted in memory
            costVector.updateCpu(sortCPUCostComputer(rawRecordsCount, uniqueKeyCount));
            outputStat.updateRecordCount((long) uniqueKeyCount);
            return;
        }
        List<Double> sortedRuns = new ArrayList<Double>();
        List<Double> rawRecordsRepresentedBySortedRuns = new ArrayList<Double>();
        // for the first (sortedRunsCount - 1) parts:
        for (int i = 0; i < sortedRunsCount - 1; i++) {
            sortedRuns.add(keysInFullMemory);
            rawRecordsRepresentedBySortedRuns.add(recordsInFullMemory);
        }
        costVector.updateCpu((sortedRunsCount - 1) * sortCPUCostComputer(recordsInFullMemory, keysInFullMemory));
        if (hasMergeGroup) {
            costVector.updateIo(2 * (sortedRunsCount - 1)
                    * Math.ceil(keysInFullMemory * recordSizeInBytes / pageSizeInBytes));
        }
        outputStat.updateRecordCount((long) ((sortedRunsCount - 1) * keysInFullMemory));

        // for the last part
        double rawRecordsForLastPart = rawRecordsCount - (sortedRunsCount - 1) * recordsInFullMemory;
        double keysInLastPart = getKeysInRecords(rawRecordsForLastPart, rawRecordsCount, uniqueKeyCount);
        sortedRuns.add(keysInLastPart);
        rawRecordsRepresentedBySortedRuns.add(rawRecordsForLastPart);
        costVector.updateCpu(sortCPUCostComputer(rawRecordsForLastPart, keysInLastPart));
        if (hasMergeGroup) {
            costVector.updateIo(2 * keysInLastPart * recordSizeInBytes / pageSizeInBytes);
        }

        outputStat.updateRecordCount((long) keysInLastPart);

        // do merge-group
        if (hasMergeGroup) {
            mergeGroupCostComputer(costVector, rawRecordsCount, uniqueKeyCount, recordSizeInBytes, sortedRuns,
                    rawRecordsRepresentedBySortedRuns, memoryInPages, pageSizeInBytes);
        }
    }

    private static double sortCPUCostComputer(double rawRecordsCount, double uniqueKeysCount) {
        double comps;
        if (uniqueKeysCount > 2) {
            comps = 2 * rawRecordsCount / uniqueKeysCount * (uniqueKeysCount + 1) * Math.log(uniqueKeysCount - 2)
                    + (rawRecordsCount / uniqueKeysCount - 1) * (2 * uniqueKeysCount - 3);
        } else {
            comps = rawRecordsCount * Math.log(rawRecordsCount) / Math.log(2) / 2;
        }

        return Math.max(0, comps);
    }

    public static double getRecordsForKeys(double keys, double totalRecords, double totalKeys) {
        if (keys >= totalKeys) {
            return totalRecords;
        }
        if (totalRecords == totalKeys) {
            return keys;
        }
        return totalRecords * (1 - Math.pow(1 - keys / totalKeys, totalKeys / totalRecords));
    }

    public static double getKeysInRecords(double recordCount, double totalRecords, double totalKeys) {
        if (totalRecords == totalKeys) {
            return recordCount;
        }
        if (recordCount >= totalRecords) {
            return totalKeys;
        }
        return totalKeys * (1 - Math.pow(1 - recordCount / totalRecords, totalRecords / totalKeys));
    }

}
