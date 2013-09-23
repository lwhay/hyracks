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
package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IFrameWriterRunGenerator;

public class RecursiveHybridHashGrouper implements IFrameWriter {

    private IFrameWriterRunGenerator grouper;

    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    protected final int[] decorFields;
    private final int framesLimit, frameSize;
    private final int tableSize;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;

    private final long inputRecordCount, outputGroupCount;
    private final int groupStateSizeInBytes;
    private final double fudgeFactor;

    private final IFrameWriter outputWriter;

    private final int hashLevelSeed;

    private int[] keyFieldsInGroupState, decorFieldsInGroupState;

    private int gracePartitions, hybridHashPartitions;

    private int maxRecursionLevel;

    public RecursiveHybridHashGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            int tableSize, long inputRecordCount, long outputGroupCount, int groupStateSizeInBytes, double fudgeFactor,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies, IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory partialMergerFactory, IAggregatorDescriptorFactory finalMergerFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, int hashLevelSeed,
            IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.frameSize = ctx.getFrameSize();
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.hashFunctionFamilies = hashFunctionFamilies;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;
        this.hashLevelSeed = hashLevelSeed;
        this.outputWriter = outputWriter;

        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;

        this.inputRecordCount = inputRecordCount;
        this.outputGroupCount = outputGroupCount;
        this.groupStateSizeInBytes = groupStateSizeInBytes;
        this.fudgeFactor = fudgeFactor;
    }

    @Override
    public void open() throws HyracksDataException {

        IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[this.hashFunctionFamilies.length];
        for (int i = 0; i < hashFunctionFactories.length; i++) {
            hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                    this.hashFunctionFamilies[i], hashLevelSeed);
        }

        gracePartitions = computeGracePartitions(framesLimit, inputRecordCount, outputGroupCount,
                groupStateSizeInBytes, fudgeFactor);
        hybridHashPartitions = computeHybridHashPartitions(framesLimit, inputRecordCount, outputGroupCount,
                groupStateSizeInBytes, gracePartitions, fudgeFactor);
        maxRecursionLevel = getMaxLevelsIfUsingSortGrouper(framesLimit, inputRecordCount, groupStateSizeInBytes);

        if (gracePartitions > 1) {
            grouper = new GracePartitioner(ctx, framesLimit, gracePartitions, keyFields, hashFunctionFactories,
                    inRecordDesc);
        } else {
            grouper = new HybridHashGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                    finalMergerFactory, inRecordDesc, outRecordDesc, false, null, tableSize, comparatorFactories,
                    hashFunctionFactories, hybridHashPartitions, true);
        }
        grouper.open();
    }

    /**
     * Compute the number of partitions to be produced by the grace partitioner. The grace partitioner partitions the
     * input data so that each partition contains no more than M^2 group states.
     * 
     * @param framesLimit
     * @param inputRecordCount
     * @param outputGroupCount
     * @param groupStateSizeInBytes
     * @param fudgeFactor
     * @return
     */
    protected int computeGracePartitions(int framesLimit, long inputRecordCount, long outputGroupCount,
            int groupStateSizeInBytes, double fudgeFactor) {
        return (int) Math.max(1, Math.ceil(outputGroupCount * groupStateSizeInBytes / frameSize * fudgeFactor
                / Math.pow(framesLimit, 2)));
    }

    protected int computeHybridHashPartitions(int framesLimit, long inputRecordCount, long outputGroupCount,
            int groupStateSizeInBytes, int gracePartitions, double fudgeFactor) {
        double partitionGroupSizeInFrames = (double) outputGroupCount / gracePartitions * groupStateSizeInBytes
                / frameSize;
        return (int) Math.max(1,
                Math.ceil((partitionGroupSizeInFrames * fudgeFactor - framesLimit) / (framesLimit - 2)));
    }

    /**
     * Compute the max level of recursion, based on the assumption that the levels of hybrid hash should
     * not be more than the levels of merging used in a sort-based approach.
     * 
     * @param framesLimit
     * @param inputRecordCount
     * @param groupStateSizeInBytes
     * @return
     */
    protected int getMaxLevelsIfUsingSortGrouper(int framesLimit, long inputRecordCount, int groupStateSizeInBytes) {
        return (int) Math.ceil(Math.log((double) inputRecordCount * groupStateSizeInBytes * frameSize / framesLimit)
                / Math.log(framesLimit));
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        grouper.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        grouper.wrapup();
        if (grouper instanceof HybridHashGrouper) {
            ((HybridHashGrouper) grouper).flushMemory(outputWriter);
        }
        List<RunFileReader> runs = grouper.getOutputRunReaders();

        if (runs.size() <= 0) {
            return;
        }

        List<Integer> runLevels = new LinkedList<Integer>();
        List<Integer> runPartitions = new LinkedList<Integer>();
        int initialPartitions = (grouper instanceof HybridHashGrouper) ? 1 : hybridHashPartitions;
        for (int i = 0; i < runs.size(); i++) {
            runLevels.add(1);
            runPartitions.add(initialPartitions);
        }

        recursiveRunProcess(runs, runLevels, runPartitions);

    }

    private void recursiveRunProcess(List<RunFileReader> runs, List<Integer> runLevels, List<Integer> runPartitions)
            throws HyracksDataException {
        if (keyFieldsInGroupState == null) {
            keyFieldsInGroupState = new int[keyFields.length];
            for (int i = 0; i < keyFields.length; i++) {
                keyFieldsInGroupState[i] = i;
            }
        }

        if (decorFieldsInGroupState == null) {
            decorFieldsInGroupState = new int[decorFields.length];
            for (int i = 0; i < decorFields.length; i++) {
                decorFieldsInGroupState[i] = i + keyFields.length;
            }
        }

        int hashLevel = runLevels.remove(0);

        IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[this.hashFunctionFamilies.length];
        for (int i = 0; i < hashFunctionFactories.length; i++) {
            hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                    this.hashFunctionFamilies[i], hashLevelSeed + hashLevel);
        }

        ByteBuffer inputBuffer = ctx.allocateFrame();

        while (runs.size() > 0) {
            RunFileReader runReader = runs.remove(0);
            int runLevel = runLevels.remove(0);
            int runPartition = runPartitions.remove(0);

            if (runLevel != hashLevel) {
                hashLevel = runLevel;
                for (int i = 0; i < hashFunctionFactories.length; i++) {
                    hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                            this.hashFunctionFamilies[i], hashLevelSeed + hashLevel);
                }
            }

            HybridHashGrouper hybridHashGrouper = new HybridHashGrouper(ctx, keyFieldsInGroupState,
                    decorFieldsInGroupState, framesLimit, partialMergerFactory, finalMergerFactory, inRecordDesc,
                    outRecordDesc, true, outputWriter, tableSize, comparatorFactories, hashFunctionFactories,
                    runPartition, true);

            hybridHashGrouper.open();
            while (runReader.nextFrame(inputBuffer)) {
                hybridHashGrouper.nextFrame(inputBuffer);
            }

            hybridHashGrouper.wrapup();

            int rawRecordsInResidentPartition = hybridHashGrouper.getRawRecordsInResidentPartition();
            int groupsInResidentPartition = hybridHashGrouper.getGroupsInResidentPartition();
            List<Integer> rawRecordsInSpillingPartitions = hybridHashGrouper.getRawRecordsInSpillingPartitions();
            List<RunFileReader> runsFromHybridHash = hybridHashGrouper.getOutputRunReaders();

            hybridHashGrouper.close();

            while (runsFromHybridHash.size() > 0) {
                RunFileReader runReaderFromHybridHash = runsFromHybridHash.remove(0);

                if (runLevel + 1 > maxRecursionLevel) {
                    // fallback to hash-sort algorithm
                    HashGroupSortMergeGrouper hashSortGrouper = new HashGroupSortMergeGrouper(ctx,
                            keyFieldsInGroupState, decorFieldsInGroupState, framesLimit, tableSize,
                            firstKeyNormalizerFactory, comparatorFactories, hashFunctionFactories, aggregatorFactory,
                            partialMergerFactory, finalMergerFactory, inRecordDesc, outRecordDesc, outputWriter);
                    hashSortGrouper.open();
                    runReaderFromHybridHash.open();
                    while (runReaderFromHybridHash.nextFrame(inputBuffer)) {
                        hashSortGrouper.nextFrame(inputBuffer);
                    }
                    hashSortGrouper.close();
                    continue;
                }

                int rawRecordsInRun = rawRecordsInSpillingPartitions.remove(0);
                int recursivePartition = computeHybridHashPartitions(framesLimit, rawRecordsInRun,
                        (int) ((double) rawRecordsInResidentPartition / groupsInResidentPartition * rawRecordsInRun),
                        groupStateSizeInBytes, 1, fudgeFactor);
                runs.add(runReaderFromHybridHash);
                runLevels.add(runLevel + 1);
                runPartitions.add(recursivePartition);
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }
}
