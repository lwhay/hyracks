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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortRunMerger;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.HybridHashUtil;

public class OriginalHybridHashGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[] keyFields, storedKeyFields;

    private final IAggregatorDescriptorFactory rawAggregatorFactory;
    private final IAggregatorDescriptorFactory internalAggregatorFactory;

    private final int framesLimit;
    private final ITuplePartitionComputerFamily rawTpcf, internalTpcf;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final int tableSize, userProvidedRecordSizeInBytes;

    private final long userProvidedRawRecordCount, userProvidedUniqueRecordCount;

    private static final double HYBRID_SWITCH_THRESHOLD = 0.8;

    private double fudgeFactor;

    private static final Logger LOGGER = Logger.getLogger(OriginalHybridHashGroupOperatorDescriptor.class
            .getSimpleName());

    public OriginalHybridHashGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            long inputSizeOfRawRecords, long inputSizeOfUniqueKeys, int tableSize, int userProvidedRecordSizeInBytes,
            double fudgeFactor, IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFamily rawTpcf,
            ITuplePartitionComputerFamily internalTpcf, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory rawAggregatorFactory, IAggregatorDescriptorFactory internalAggregatorFactory,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.fudgeFactor = fudgeFactor;
        this.userProvidedRawRecordCount = inputSizeOfRawRecords;
        this.userProvidedUniqueRecordCount = inputSizeOfUniqueKeys;

        this.userProvidedRecordSizeInBytes = userProvidedRecordSizeInBytes;

        if (framesLimit <= 3) {
            /**
             * Minimum of 3 frames: 2 for in-memory hash table (1 header page and 1 content page), and 1 for output
             * aggregation results.
             */
            throw new IllegalStateException("frame limit should at least be 3, but it is " + framesLimit + "!");
        }

        storedKeyFields = new int[keyFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        this.rawAggregatorFactory = rawAggregatorFactory;
        this.internalAggregatorFactory = internalAggregatorFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.rawTpcf = rawTpcf;
        this.internalTpcf = internalTpcf;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.tableSize = tableSize;

        /**
         * Set the record descriptor. Note that since this operator is a unary
         * operator, only the first record descriptor is used here.
         */
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, final int nPartitions)
            throws HyracksDataException {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);

        final int frameSize = ctx.getFrameSize();

        fudgeFactor *= AbstractHybridHashDynamicPartitionGroupHashTable.getHashTableOverheadFactor(tableSize,
                userProvidedRecordSizeInBytes, frameSize, framesLimit);

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            AbstractHybridHashDynamicPartitionGroupHashTable topProcessor;

            int observedInputSizeInRawTuples;

            int observedInputSizeInFrames, maxRecursiveLevels;

            int userProvidedUniqueGroupSizeInFrames;

            IAggregatorDescriptor rawAggregator = rawAggregatorFactory.createAggregator(ctx, inRecDesc,
                    recordDescriptors[0], keyFields, storedKeyFields);

            IAggregatorDescriptor internalAggregator = internalAggregatorFactory.createAggregator(ctx,
                    recordDescriptors[0], recordDescriptors[0], storedKeyFields, storedKeyFields);

            ByteBuffer readAheadBuf;

            int topPartitions;

            /**
             * Compute the initial partition fan-out. The initial partition number is computed using the original
             * hybrid-hash formula. Then the following cases are considered:
             * <p/>
             * - if the partition number is no larger than 1, use 2 partitions, in case that all input will be partitioned into a single partition and spilled.<br/>
             * - if the partition number is larger than the available memory (total memory minus the header pages for hash table and the output buffer), use (framesLimit - 1) partitions to do partition. <br/>
             * 
             * @param tableSize
             * @param framesLimit
             * @param inputSize
             * @param partitionInOperator
             * @param factor
             * @return
             */
            private int getNumberOfPartitions(int tableSize, int framesLimit, int inputSizeOfUniqueKeysInFrames,
                    double factor) {

                int hashtableHeaderPages = OriginalHybridHashGroupHashTable.getHeaderPages(tableSize, frameSize);
                int numberOfPartitions = HybridHashUtil.hybridHashPartitionComputer(
                        (int) Math.ceil(inputSizeOfUniqueKeysInFrames), framesLimit, factor) + 1;

                // if the partition number is more than the available hash table contents, do pure partition.
                if (numberOfPartitions >= framesLimit - hashtableHeaderPages - 1) {
                    numberOfPartitions = framesLimit;
                }

                return numberOfPartitions;
            }

            @Override
            public void open() throws HyracksDataException {

                // estimate the number of unique keys for this partition, given the total raw record count and unique record count
                long estimatedNumberOfUniqueGroupCountForThisPartition = HybridHashUtil
                        .getEstimatedPartitionSizeOfUniqueKeys(userProvidedRawRecordCount,
                                userProvidedUniqueRecordCount, nPartitions);

                userProvidedUniqueGroupSizeInFrames = (int) Math
                        .ceil((float) estimatedNumberOfUniqueGroupCountForThisPartition * userProvidedRecordSizeInBytes
                                / frameSize);

                ctx.getCounterContext().getCounter("optional.levels.0.userProvidedInputSizeInFrames", true)
                        .set(userProvidedUniqueGroupSizeInFrames);

                // calculate the number of partitions (including partition 0)
                topPartitions = getNumberOfPartitions(tableSize, framesLimit, userProvidedUniqueGroupSizeInFrames,
                        fudgeFactor);

                ctx.getCounterContext().getCounter("optional.levels.0.partitions", true).set(topPartitions);

                topProcessor = new OriginalHybridHashGroupHashTable(ctx, framesLimit, tableSize, topPartitions, 0,
                        keyFields, comparators, rawTpcf, internalTpcf, rawAggregator, internalAggregator, inRecDesc,
                        recordDescriptors[0], writer);

                writer.open();
                topProcessor.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                observedInputSizeInRawTuples += buffer.getInt(buffer.capacity() - 4);
                topProcessor.nextFrame(buffer, true);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                // estimate the maximum recursive levels
                maxRecursiveLevels = (int) Math.max(
                        Math.ceil(Math.log(observedInputSizeInFrames * fudgeFactor) / Math.log(framesLimit)) + 1, 1);

                finishAndRecursion(topProcessor, observedInputSizeInRawTuples, userProvidedUniqueRecordCount, 0);

                writer.close();
            }

            private void processRunFile(List<IFrameReader> runReaders, List<Integer> runAggregatedPages,
                    int inputKeyCardinality, int level) throws HyracksDataException {

                // partition counts for this run file
                int numOfPartitions = getNumberOfPartitions(tableSize, framesLimit,
                        (int) Math.ceil((double) inputKeyCardinality * userProvidedRecordSizeInBytes / frameSize),
                        fudgeFactor);

                AbstractHybridHashDynamicPartitionGroupHashTable ht = new OriginalHybridHashGroupHashTable(ctx,
                        framesLimit, tableSize, numOfPartitions, level, keyFields, comparators, rawTpcf, internalTpcf,
                        rawAggregator, internalAggregator, inRecDesc, recordDescriptors[0], writer);

                ht.open();

                int observedRawRecordCount = 0;

                if (readAheadBuf == null) {
                    readAheadBuf = ctx.allocateFrame();
                }

                for (int i = 0; i < runReaders.size(); i++) {
                    runReaders.get(i).open();
                    int pageIndex = 0;
                    while (pageIndex < runAggregatedPages.get(i) && runReaders.get(i).nextFrame(readAheadBuf)) {
                        observedRawRecordCount += readAheadBuf.getInt(readAheadBuf.capacity() - 4);
                        ht.nextFrame(readAheadBuf, false);
                        pageIndex++;
                    }
                }

                for (int i = 0; i < runReaders.size(); i++) {
                    while (runReaders.get(i).nextFrame(readAheadBuf)) {
                        observedRawRecordCount += readAheadBuf.getInt(readAheadBuf.capacity() - 4);
                        ht.nextFrame(readAheadBuf, true);
                    }
                    runReaders.get(i).close();
                }

                runReaders.clear();

                finishAndRecursion(ht, observedRawRecordCount, inputKeyCardinality, level);

            }

            private void finishAndRecursion(AbstractHybridHashDynamicPartitionGroupHashTable ht, long inputRecordCount,
                    long inputKeyCardinality, int level) throws HyracksDataException {

                ht.finishup();

                List<IFrameReader> generatedRunReaders = ht.getSpilledRuns();
                List<Integer> generatedRunAggregatedPages = ht.getSpilledRunsAggregatedPages();
                List<Integer> partitionRawRecords = ht.getSpilledRunsSizeInRawTuples();

                // adjust the key cardinality
                int observedUniqueGroups = ht.getHashedUniqueKeys();
                if (observedUniqueGroups > inputKeyCardinality) {
                    inputKeyCardinality = observedUniqueGroups;
                }

                ht.close();
                ht = null;

                if (generatedRunReaders.size() <= 0) {
                    return;
                }

                List<IFrameReader> runsToProcessByPartTune = new LinkedList<IFrameReader>();
                List<Integer> runsToProcessAggregatedPages = new LinkedList<Integer>();
                while (!generatedRunReaders.isEmpty()) {
                    IFrameReader subRunReader = generatedRunReaders.remove(0);
                    int subRunAggPages = generatedRunAggregatedPages.remove(0);

                    int subPartitionRawRecords = partitionRawRecords.remove(0);

                    int subRunCardinality = (int) Math.ceil((double) inputKeyCardinality * subPartitionRawRecords
                            / inputRecordCount);

                    // Note that it is important to check the maxRecursiveLevels, as it is possible in the partition
                    // tuning, all runs are processed together but the partial aggregation results can cause spill
                    // (so there would be no effect in the hybrid-hash part) but still spill
                    if (subRunCardinality > inputKeyCardinality * HYBRID_SWITCH_THRESHOLD || level > maxRecursiveLevels) {
                        LOGGER.warning("Hybrid-hash falls back to hash-sort algorithm! (" + level + ":"
                                + maxRecursiveLevels + ")");
                        fallBackAlgorithm(subRunReader, subRunAggPages, level + 1);

                    } else {
                        // recursively processing
                        runsToProcessByPartTune.add(subRunReader);
                        runsToProcessAggregatedPages.add(subRunAggPages);
                        processRunFile(runsToProcessByPartTune, runsToProcessAggregatedPages, subRunCardinality,
                                level + 1);
                        runsToProcessByPartTune.clear();
                        runsToProcessAggregatedPages.clear();
                    }
                }

            }

            /**
             * Fall back to hash-sort algorithm for the given run file.
             * 
             * @param recurRunReader
             * @param runLevel
             * @throws HyracksDataException
             */
            private void fallBackAlgorithm(IFrameReader recurRunReader, int runAggregatedPages, int runLevel)
                    throws HyracksDataException {
                FrameTupleAccessor runFramePartialTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),
                        recordDescriptors[0]);
                FrameTupleAccessor runFrameRawAggregatedTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),
                        inRecDesc);
                HybridHashSortGroupHashTableForHybridHashFallback hhsTable = new HybridHashSortGroupHashTableForHybridHashFallback(
                        ctx, framesLimit, tableSize, keyFields, comparators, rawTpcf.createPartitioner(runLevel + 2),
                        internalTpcf.createPartitioner(runLevel + 2),
                        firstNormalizerFactory.createNormalizedKeyComputer(), rawAggregatorFactory.createAggregator(
                                ctx, inRecDesc, recordDescriptors[0], keyFields, storedKeyFields),
                        internalAggregatorFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                                storedKeyFields, storedKeyFields), recordDescriptors[0], recordDescriptors[0]);

                recurRunReader.open();
                int pageCount = 0;

                if (readAheadBuf == null) {
                    readAheadBuf = ctx.allocateFrame();
                }

                while (recurRunReader.nextFrame(readAheadBuf)) {
                    FrameTupleAccessor accessor = (pageCount >= runAggregatedPages) ? runFrameRawAggregatedTupleAccessor
                            : runFramePartialTupleAccessor;
                    accessor.reset(readAheadBuf);
                    int tupleCount = accessor.getTupleCount();
                    for (int j = 0; j < tupleCount; j++) {
                        hhsTable.insert(accessor, j, pageCount >= runAggregatedPages);
                    }
                    pageCount++;
                }

                recurRunReader.close();
                hhsTable.finishup();

                LinkedList<RunFileReader> hhsRuns = hhsTable.getRunFileReaders();

                if (hhsRuns.isEmpty()) {
                    hhsTable.flushHashtableToOutput(writer);
                    hhsTable.close();
                } else {
                    hhsTable.close();
                    HybridHashSortRunMerger hhsMerger = new HybridHashSortRunMerger(ctx, hhsRuns, storedKeyFields,
                            comparators, recordDescriptors[0], internalTpcf.createPartitioner(runLevel + 2),
                            internalAggregatorFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                                    storedKeyFields, storedKeyFields), framesLimit, tableSize, writer, false);
                    hhsMerger.process();
                }

                hhsTable = null;
            }

        };
    }

}
