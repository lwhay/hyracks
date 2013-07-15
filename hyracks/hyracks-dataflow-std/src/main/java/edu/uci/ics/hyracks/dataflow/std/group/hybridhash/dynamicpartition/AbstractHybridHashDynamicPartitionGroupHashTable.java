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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.FrameMemManager;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.FrameTupleAccessorForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.FrameTupleAppenderForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.TupleAccessHelper;

public abstract class AbstractHybridHashDynamicPartitionGroupHashTable {
    protected static final int INT_SIZE = 4;

    protected static final int END_REF = -1;

    protected final IHyracksTaskContext ctx;

    protected final IFrameWriter outputWriter;

    /**
     * key fields in input data
     */
    protected final int[] keys;

    /**
     * key fields in intermediate results
     */
    protected final int[] internalKeys;

    /**
     * comparators for all grouping fields
     */
    protected final IBinaryComparator[] comparators;

    /**
     * hash table slot count
     */
    protected final int tableSize;

    /**
     * memory budget
     */
    protected final int framesLimit;

    /**
     * frame size in bytes
     */
    protected final int frameSize;

    protected final int procLevel;

    /**
     * frame manager
     */
    protected final FrameMemManager frameManager;

    /**
     * header frames
     */
    protected final int[] headers;

    /**
     * output buffer (write-behind buffer)
     */
    protected int outputBuffer;

    /**
     * number of partitions
     */
    protected final int numOfPartitions;

    /**
     * output buffers for spilled partitions
     */
    protected final int[] partitionTopPages;

    protected final int[] partitionAggregatedPages;

    /**
     * The flags whether partitions are in in-memory state (false) or spilling state (true)
     */
    private final boolean[] partitionStates;

    /**
     * match pointer for hash table lookup
     */
    protected int[] matchPointer = new int[2];

    /**
     * Hash functions. One for the input and one for the aggregation partial results in memory.
     */
    protected final ITuplePartitionComputer rawPartitionTpc, intermediatePartitionTpc;

    protected ITuplePartitionComputer rawHashTpc, intermediateHashTpc;

    /**
     * hash record accessor
     */
    protected final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    protected final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;

    protected final FrameTupleAccessor inputFrameTupleAccessor, inputFramePartialAggregatedTupleAccessor;

    /**
     * appender used to spill records from the spilled partitions
     */
    protected final FrameTupleAppender spilledPartitionAppender;

    protected final FrameTupleAccessor spilledPartitionFrameAccessor;

    protected final FrameTupleAppender outputAppender;

    protected final FrameTupleAppenderForGroupHashtable internalAppender;

    /**
     * tuple access helper
     */
    protected final TupleAccessHelper tupleAccessHelper;

    protected final IAggregatorDescriptor rawAggregator, intermediateAggregator;

    protected final AggregateState[] partitionAggregateStates;

    protected RunFileWriter[] runWriters;

    /**
     * The state of the current input state:
     * <p/>
     * - 0: initialized but nothing inserted yet - 1: input partial results - 2: input raw results.
     */
    protected int inputRecordState;

    protected final int partition0KeyRange;

    /**************
     * For statistic information
     ****************/

    protected LinkedList<Integer> partitionRawRecords;
    protected LinkedList<IFrameReader> runReaders;
    protected LinkedList<Integer> runsAggregatedPages;
    protected LinkedList<Integer> partitionSizeInPages;
    protected int directFlushedSizeInPages, hashedKeys, hashedRawRecords;

    /**
     * for profiling
     */
    protected long hashInitCount, hashSuccComparisonCount, hashUnsuccComparisonCount, comparsionCount;
    protected int[] partitionSizeInFrames, partitionRawRecordCounts;
    protected boolean firstOutputMarker = false;

    public AbstractHybridHashDynamicPartitionGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int numberOfPartitions, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily rawTpcf, ITuplePartitionComputerFamily intermediateTpcf,
            IAggregatorDescriptor rawAggregator, IAggregatorDescriptor intermediateAggregator,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, IFrameWriter outputWriter)
            throws HyracksDataException {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.framesLimit = frameLimits;
        this.frameSize = ctx.getFrameSize();
        this.procLevel = procLevel;

        this.frameManager = new FrameMemManager(framesLimit, ctx);

        this.rawPartitionTpc = rawTpcf.createPartitioner(procLevel);
        this.intermediatePartitionTpc = intermediateTpcf.createPartitioner(procLevel);
        this.rawHashTpc = rawPartitionTpc;
        this.intermediateHashTpc = intermediatePartitionTpc;

        this.keys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.outputWriter = outputWriter;

        this.rawAggregator = rawAggregator;
        this.intermediateAggregator = intermediateAggregator;

        this.comparators = comparators;

        this.hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inRecDesc);
        this.inputFramePartialAggregatedTupleAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        this.spilledPartitionFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        this.internalTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.spilledPartitionAppender = new FrameTupleAppender(frameSize);
        this.outputAppender = new FrameTupleAppender(frameSize);

        this.tupleAccessHelper = new TupleAccessHelper(outRecDesc);

        this.internalAppender = new FrameTupleAppenderForGroupHashtable(frameSize);

        this.matchPointer = new int[2];

        // initialize the hash table
        if (numberOfPartitions < 1) {
            throw new HyracksDataException("At least one partition is required for hybrid-hash algorithm");
        }
        int headerPageCount = getHeaderSize(tableSize, frameSize);

        // if the expected partition numbers is larger than the pages left 
        // from the hash table, then do pure partition.
        if (numberOfPartitions - 1 >= framesLimit - 1 - headerPageCount) {
            // pure partition

            // - no output buffer is needed
            this.outputBuffer = -1;

            this.numOfPartitions = framesLimit;
            this.headers = new int[0];
            this.partitionStates = new boolean[numOfPartitions];
            this.partitionTopPages = new int[numOfPartitions];
            for (int i = 0; i < numOfPartitions; i++) {
                partitionStates[i] = true;
                this.partitionTopPages[i] = -1;
            }
            ctx.getCounterContext().getCounter("optional.levels." + procLevel + ".type", true).update(1);
        } else {
            // hybrid-hash-partition

            // - reserve the output buffer for hash table
            this.outputBuffer = frameManager.allocateFrame();
            if (this.outputBuffer < 0) {
                throw new HyracksDataException("Failed to allocate a frame as the output buffer");
            }

            this.numOfPartitions = numberOfPartitions;
            this.headers = new int[headerPageCount];
            // reserve space for headers
            int allocatedFrameIdx = frameManager.bulkAllocate(headers.length);

            if (allocatedFrameIdx < 0) {
                throw new HyracksDataException("Failed to allocate frames for the hash table header");
            }

            for (int i = 0; i < headers.length; i++) {
                headers[i] = allocatedFrameIdx;
                resetHeader(headers[i]);
                allocatedFrameIdx = frameManager.getNextFrame(allocatedFrameIdx);
            }

            this.partitionStates = new boolean[numOfPartitions];
            // initialize the top page for each partition: no page is assigned at the beginning
            this.partitionTopPages = new int[numOfPartitions];
            for (int i = 0; i < numOfPartitions; i++) {
                partitionStates[i] = false;
                this.partitionTopPages[i] = -1;
            }
            ctx.getCounterContext().getCounter("optional.levels." + procLevel + ".type", true).update(100000000);
        }

        double partition0RangeRatio = 1;

        if (numberOfPartitions > 0) {
            partition0RangeRatio = ((double) (framesLimit - numOfPartitions))
                    / (framesLimit - numOfPartitions + numOfPartitions * frameLimits);
        }

        this.partition0KeyRange = (int) (Integer.MAX_VALUE * partition0RangeRatio);

        this.inputRecordState = 0;

        this.runWriters = new RunFileWriter[this.numOfPartitions];
        this.partitionAggregatedPages = new int[numOfPartitions];
        this.partitionAggregateStates = new AggregateState[this.numOfPartitions];

        this.partitionSizeInFrames = new int[this.numOfPartitions];
        this.partitionRawRecordCounts = new int[this.numOfPartitions];
    }

    /**
     * Compute the partition id for the given hash key.
     * 
     * @param hashKey
     * @return
     */
    protected int partition(int hashKey) {
        // for partition-0: following the hybrid-hash formula
        if (hashKey < partition0KeyRange || numOfPartitions == 1)
            return 0;
        // the other partitions are uniformly distributed
        return (hashKey - partition0KeyRange) % (numOfPartitions - 1) + 1;
    }

    /**
     * Reset the header page
     * 
     * @param headerFrameIndex
     * @throws HyracksDataException 
     */
    protected void resetHeader(int headerFrameIndex) throws HyracksDataException {
        ByteBuffer buf = frameManager.getFrame(headerFrameIndex);
        for (int i = 0; i < frameSize; i += INT_SIZE) {
            buf.putInt(i, -1);
        }
    }

    /**
     * Get the header frame index of the given hash table entry. </p>
     * Note that compared with (entry / (frameSize / (2 * INT_SIZE))), the current implementation
     * can handle the integer overflowing case nicely.</p>
     * 
     * @param entry
     * @return
     */
    protected int getHeaderFrameIndex(int entry) {
        int frameIndex = (entry / frameSize * 2 * INT_SIZE) + (entry % frameSize * 2 * INT_SIZE / frameSize);
        return frameIndex;
    }

    /**
     * Get the tuple index of the given hash table entry. </p>
     * Note that compared with (entry % (frameSize / (2 * INT_SIZE))), the current implementation
     * can handle the integer overflowing case nicely.</p>
     * 
     * @param entry
     * @return
     */
    protected int getHeaderTupleIndex(int entry) {
        int offset = (entry % frameSize) * 2 * INT_SIZE % frameSize;
        return offset;
    }

    /**
     * Get the tuple start offset for the given frame and tuple index.
     * 
     * @param frameIndex
     * @param tupleIndex
     * @return
     * @throws HyracksDataException 
     */
    protected int getTupleStartOffset(int frameIndex, int tupleIndex) throws HyracksDataException {
        if (tupleIndex == 0) {
            return 0;
        } else {
            return frameManager.getFrame(frameIndex).getInt(frameSize - INT_SIZE - tupleIndex * INT_SIZE);
        }
    }

    /**
     * do a hash table lookup for the given record.
     * 
     * @param entry
     * @param accessor
     * @param tupleIndex
     * @return
     * @throws HyracksDataException 
     */
    protected boolean findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {

        int comps = 0;

        int tupleStartOffset = accessor.getTupleStartOffset(tupleIndex);

        // reset the match pointer
        matchPointer[0] = -1;
        matchPointer[1] = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (!frameManager.isFrameInitialized(headers[headerFrameIndex])) {

            hashInitCount++;

            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = frameManager.getFrame(headers[headerFrameIndex]).getInt(headerFrameOffset);
        int entryTupleStartOffset = frameManager.getFrame(headers[headerFrameIndex]).getInt(
                headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {

            matchPointer[0] = entryFrameIndex;
            matchPointer[1] = entryTupleStartOffset;

            hashtableRecordAccessor.reset(frameManager.getFrame(entryFrameIndex));
            comps++;
            if (compare(accessor, tupleStartOffset, hashtableRecordAccessor, entryTupleStartOffset) == 0) {

                hashSuccComparisonCount += comps;

                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = tupleAccessHelper.getTupleEndOffset(frameManager.getFrame(entryFrameIndex),
                    entryTupleStartOffset);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = frameManager.getFrame(prevFrameIndex).getInt(refOffset);
            entryTupleStartOffset = frameManager.getFrame(prevFrameIndex).getInt(refOffset + INT_SIZE);
        }

        if (comps == 0) {
            hashInitCount++;
        }
        hashUnsuccComparisonCount += comps;

        return false;
    }

    /**
     * insert a record into a spilled partition. The inserted record is directly copied to the output buffer.
     * If the output buffer is full, depending on whether all partitions have been spilled or not, either the
     * output buffer will be flushed if there is in-memory partition, or a new frame is tried to be allocated.
     * 
     * @param pid
     * @param accessor
     * @param tupleIndex
     * @throws HyracksDataException
     */
    protected void insertSpilledPartition(FrameTupleAccessor accessor, int tupleIndex, int pid)
            throws HyracksDataException {
        spilledPartitionAppender.reset(frameManager.getFrame(partitionTopPages[pid]), false);
        if (!spilledPartitionAppender.append(accessor, tupleIndex)) {
            if (isAllPartitionsSpilled()) {
                // if all partitions are spilled, do dynamic allocation
                int newFrameIdx = frameManager.allocateFrame();
                if (newFrameIdx >= 0) {
                    frameManager.setNextFrame(newFrameIdx, partitionTopPages[pid]);
                    partitionTopPages[pid] = newFrameIdx;
                    partitionSizeInFrames[pid]++;
                } else {
                    flushSpilledPartition(pid);
                }
            } else {
                flushSpilledPartition(pid);
            }
            spilledPartitionAppender.reset(frameManager.getFrame(partitionTopPages[pid]), true);
            if (!spilledPartitionAppender.append(accessor, tupleIndex)) {
                throw new HyracksDataException(
                        "Failed to spill a record to the run file: record is too large to be in a frame.");
            }
        }
    }

    /**
     * Flush a spilled partition into its run file by direct copying.
     * 
     * @param pid
     * @throws HyracksDataException
     */
    protected void flushSpilledPartition(int pid) throws HyracksDataException {
        int currentPage = partitionTopPages[pid];
        while (currentPage >= 0) {

            if (frameManager.getFrame(currentPage).getInt(frameSize - INT_SIZE) > 0) {

                if (runWriters[pid] == null) {
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                AbstractHybridHashDynamicPartitionGroupHashTable.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    runWriters[pid] = new RunFileWriter(runFile, ctx.getIOManager());
                    runWriters[pid].open();
                }

                FrameUtils.flushFrame(frameManager.getFrame(currentPage), runWriters[pid]);

                if (inputRecordState == 1) {
                    partitionAggregatedPages[pid]++;
                }
            }

            int nextPage = frameManager.getNextFrame(currentPage);
            if (nextPage >= 0) {
                // recycle the current page
                frameManager.recycleFrame(currentPage);
                currentPage = nextPage;
            } else {
                // no more pages to flush
                break;
            }
        }

        // reset the current page
        frameManager.resetFrame(currentPage);
        partitionTopPages[pid] = currentPage;

    }

    /**
     * Compare a raw record with a record in a hash table.
     * 
     * @param accessor
     * @param tupleStartOffset
     * @param hashAccessor
     * @param hashTupleStartOffset
     * @return
     */
    private int compare(FrameTupleAccessor accessor, int tupleStartOffset,
            FrameTupleAccessorForGroupHashtable hashAccessor, int hashTupleStartOffset) {

        int[] ks = inputRecordState == 1 ? internalKeys : keys;

        comparsionCount++;
        int fStartOffset0 = accessor.getFieldSlotsLength() + tupleStartOffset;

        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + hashTupleStartOffset;

        for (int i = 0; i < ks.length; ++i) {
            int fStart0 = ks[i] == 0 ? 0 : accessor.getBuffer().getInt(tupleStartOffset + (ks[i] - 1) * 4);
            int fEnd0 = accessor.getBuffer().getInt(tupleStartOffset + ks[i] * 4);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = internalKeys[i] == 0 ? 0 : hashAccessor.getBuffer().getInt(
                    hashTupleStartOffset + (internalKeys[i] - 1) * 4);
            int fEnd1 = hashAccessor.getBuffer().getInt(hashTupleStartOffset + internalKeys[i] * 4);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0, hashAccessor
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /**
     * Spill an in-memory partition to the output. Depending on the flag, it can flush the partition
     * into either the final output or a run file.
     * <p/>
     * Note that at the end of this call the global output buffer will be emptied, to avoid mixing records from different partitions.
     * <p/>
     * Also notice that if a hash table slot can span multiple partitions, after spilling partition the slot reference link list may be broken. This means that in this implementation of spilling a partition the link list reference is not maintained.
     * <p/>
     * 
     * @param partToSpill
     * @param isFinalOutput
     * @throws HyracksDataException
     */
    protected void spillPartition(int partToSpill, boolean isFinalOutput) throws HyracksDataException {

        ByteBuffer buf;

        if (partitionTopPages[partToSpill] < 0) {
            return;
        }

        outputAppender.reset(frameManager.getFrame(outputBuffer), true);

        while (partitionTopPages[partToSpill] >= 0) {
            buf = frameManager.getFrame(partitionTopPages[partToSpill]);
            spilledPartitionFrameAccessor.reset(buf);
            for (int j = 0; j < spilledPartitionFrameAccessor.getTupleCount(); j++) {
                outputTupleBuilder.reset();
                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(spilledPartitionFrameAccessor, j, internalKeys[k]);
                }

                if (isFinalOutput) {
                    intermediateAggregator.outputFinalResult(outputTupleBuilder, spilledPartitionFrameAccessor, j,
                            partitionAggregateStates[partToSpill]);
                } else {
                    intermediateAggregator.outputPartialResult(outputTupleBuilder, spilledPartitionFrameAccessor, j,
                            partitionAggregateStates[partToSpill]);
                }

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    if (isFinalOutput)
                        FrameUtils.flushFrame(frameManager.getFrame(outputBuffer), outputWriter);
                    else {
                        // initialize the run file if it is null
                        if (runWriters[partToSpill] == null) {
                            FileReference runFile;
                            try {
                                runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                        AbstractHybridHashDynamicPartitionGroupHashTable.class.getSimpleName());
                            } catch (IOException e) {
                                throw new HyracksDataException(e);
                            }
                            runWriters[partToSpill] = new RunFileWriter(runFile, ctx.getIOManager());
                            runWriters[partToSpill].open();
                        }
                        FrameUtils.flushFrame(frameManager.getFrame(outputBuffer), runWriters[partToSpill]);
                        // add one more aggregated page
                        partitionAggregatedPages[partToSpill]++;
                    }

                    outputAppender.reset(frameManager.getFrame(outputBuffer), true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }
            }

            int nextFrameToSpill = frameManager.getNextFrame(partitionTopPages[partToSpill]);
            if (nextFrameToSpill >= 0) {
                frameManager.recycleFrame(partitionTopPages[partToSpill]);
                partitionTopPages[partToSpill] = nextFrameToSpill;
            } else {
                break;
            }
        }

        frameManager.getFrame(partitionTopPages[partToSpill]).putInt(frameSize - INT_SIZE, 0);

        // clean up the records in the output buffer.
        if (outputAppender.getTupleCount() > 0) {
            if (isFinalOutput)
                FrameUtils.flushFrame(frameManager.getFrame(outputBuffer), outputWriter);
            else {
                // initialize the run file if it is null
                if (runWriters[partToSpill] == null) {
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                AbstractHybridHashDynamicPartitionGroupHashTable.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    runWriters[partToSpill] = new RunFileWriter(runFile, ctx.getIOManager());
                    runWriters[partToSpill].open();
                }
                FrameUtils.flushFrame(frameManager.getFrame(outputBuffer), runWriters[partToSpill]);
                // add one more aggregated page
                partitionAggregatedPages[partToSpill]++;
            }
            outputAppender.reset(frameManager.getFrame(outputBuffer), true);
        }
    }

    public void nextFrame(ByteBuffer buffer, boolean isRawInput) throws HyracksDataException {
        FrameTupleAccessor accessor = (isRawInput) ? inputFrameTupleAccessor : inputFramePartialAggregatedTupleAccessor;
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            insert(accessor, i, isRawInput);
        }
    }

    public void close() throws HyracksDataException {
        for (int i = 0; i < numOfPartitions; i++) {
            if (partitionAggregateStates[i] != null)
                partitionAggregateStates[i].close();
        }

        frameManager.close();

        for (int i = 0; i < runWriters.length; i++) {
            if (runWriters[i] != null) {
                runWriters[i].close();
                runWriters[i] = null;
            }
        }

        runWriters = null;

        ctx.getCounterContext().getCounter("must.hash.succ.comps", true).update(hashSuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.unsucc.comps", true).update(hashUnsuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.slot.init", true).update(hashInitCount);
    }

    public List<IFrameReader> getSpilledRuns() throws HyracksDataException {
        return runReaders;
    }

    public List<Integer> getSpilledRunsSizeInRawTuples() throws HyracksDataException {
        return partitionRawRecords;
    }

    public List<Integer> getSpilledRunsAggregatedPages() throws HyracksDataException {
        return runsAggregatedPages;
    }

    public int getHashedUniqueKeys() throws HyracksDataException {
        return hashedKeys;
    }

    public int getHashedRawRecords() throws HyracksDataException {
        return hashedRawRecords;
    }

    /**
     * Get the number of header pages needed by the hash table.
     * 
     * @param hashtableSlots
     * @param frameSize
     * @return
     */
    public static int getHeaderPages(int hashtableSlots, int frameSize) {
        return (int) Math.ceil((double) hashtableSlots * 2 * INT_SIZE / frameSize);
    }

    /**
     * Get the hash table overhead ratio
     * 
     * @param hashtableSlots
     * @param estimatedRecordSize
     * @param frameSize
     * @param memorySizeInFrames
     * @return
     */
    public static double getHashTableOverheadFactor(int hashtableSlots, int estimatedRecordSize, int frameSize,
            int memorySizeInFrames) {
        int pagesForRecord = memorySizeInFrames - getHeaderPages(hashtableSlots, frameSize) - 1;
        int recordsInHashtable = pagesForRecord * ((int) (frameSize / (estimatedRecordSize + 2 * INT_SIZE)));

        return (double) memorySizeInFrames * frameSize / recordsInHashtable / estimatedRecordSize;
    }

    protected abstract void insert(FrameTupleAccessor accessor, int tupleIndex, boolean isRawInput)
            throws HyracksDataException;

    protected abstract void chooseResidentPartitionToFlush() throws HyracksDataException;

    public abstract void finishup() throws HyracksDataException;

    public abstract void open() throws HyracksDataException;

    public abstract void fail() throws HyracksDataException;

    protected abstract boolean isAllPartitionsSpilled();

    protected boolean isPartitionSpilled(int pid) {
        return partitionStates[pid];
    }

    protected void setPartitionSpilled(int pid) {
        partitionStates[pid] = true;
    }

    protected abstract int getHeaderSize(int tableSize, int frameSize);

}
