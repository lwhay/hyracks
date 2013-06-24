/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash;

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
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.FrameTupleAccessorForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.FrameTupleAppenderForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.TupleAccessHelper;

public class HybridHashGroupHashTable implements IFrameWriter {

    private static final int HEADER_REF_EMPTY = -1;

    private static final int INT_SIZE = 4;

    private static final int MINI_BLOOM_FILTER_BYTE = 1;

    private static final int PRIME_FUNC_COUNT = 3;

    private final boolean useMiniBloomFilter;

    private IHyracksTaskContext ctx;

    private final int frameSize;

    private final int framesLimit;

    private final int tableSize;

    private final int numOfPartitions;

    private final IFrameWriter outputWriter;

    private final IBinaryComparator[] comparators;

    /**
     * index for keys
     */
    private final int[] inputKeys, internalKeys;

    private final RecordDescriptor inputRecordDescriptor, outputRecordDescriptor;

    /**
     * hash partitioner for hashing
     */
    private final ITuplePartitionComputer hashComputer;

    /**
     * Hashtable headers
     */
    private ByteBuffer[] headers;

    /**
     * buffers for hash table
     */
    private ByteBuffer[] contents;

    /**
     * output buffers for spilled partitions
     */
    private ByteBuffer[] spilledPartOutputBuffers;

    /**
     * run writers for spilled partitions
     */
    private RunFileWriter[] spilledPartRunWriters;

    private int[] spilledPartRunSizeArrayInFrames;
    private int[] spilledPartRunSizeArrayInTuples;

    private List<IFrameReader> spilledPartRunReaders = null;
    private List<Integer> spilledRunAggregatedPages = null;
    private List<Integer> spilledPartRunSizesInFrames = null;
    private List<Integer> spilledPartRunSizesInTuples = null;

    /**
     * index of the current working buffer in hash table
     */
    private int currentHashTableFrame;

    /**
     * Aggregation state
     */
    private AggregateState htAggregateState;

    /**
     * the aggregator
     */
    private final IAggregatorDescriptor aggregator;

    /**
     * records inserted into the in-memory hash table (for hashing and aggregation)
     */
    private int hashedRawRecords = 0;

    /**
     * in-memory part size in tuples
     */
    private int hashedKeys = 0;

    /**
     * Hash table tuple pointer
     */
    private int[] matchPointer;

    /**
     * Frame tuple accessor for input data frames
     */
    private FrameTupleAccessor inputFrameTupleAccessor;

    /**
     * The helper for tuple accessing
     */
    private final TupleAccessHelper tupleAccessHelper;

    /**
     * flag for whether the hash table if full
     */
    private boolean isHashtableFull;

    /**
     * flag for only partition (no aggregation and hashing)
     */
    private boolean isPartitionOnly;

    /**
     * Tuple accessor for hash table contents
     */
    private FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    /**
     * Tuple builder used for the intermediate group-by records
     */
    private ArrayTupleBuilder internalTupleBuilder;

    /**
     * Appender for spilled partitions
     */
    private FrameTupleAppender spilledPartInsertAppender;

    /**
     * Appender for the hash table (so that the hash reference can be correctly maintained)
     */
    private FrameTupleAppenderForGroupHashtable htInsertAppender;

    private final int headerEntryPerFrame;

    public HybridHashGroupHashTable(IHyracksTaskContext ctx, int framesLimit, int tableSize, int numOfPartitions,
            int[] keys, int hashSeedOffset, IBinaryComparator[] comparators, ITuplePartitionComputerFamily tpcFamily,
            IAggregatorDescriptor aggregator, RecordDescriptor inputRecordDescriptor,
            RecordDescriptor outputRecordDescriptor, IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.frameSize = ctx.getFrameSize();
        this.tableSize = tableSize;
        this.framesLimit = framesLimit;
        this.numOfPartitions = numOfPartitions;

        if (this.numOfPartitions > 1) {
            this.useMiniBloomFilter = true;
        } else {
            this.useMiniBloomFilter = false;
        }

        this.inputKeys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.comparators = comparators;

        this.inputRecordDescriptor = inputRecordDescriptor;
        this.outputRecordDescriptor = outputRecordDescriptor;

        this.tupleAccessHelper = new TupleAccessHelper(outputRecordDescriptor);

        this.outputWriter = outputWriter;

        this.hashComputer = tpcFamily.createPartitioner(hashSeedOffset);

        this.aggregator = aggregator;

        this.headerEntryPerFrame = frameSize / (2 * INT_SIZE + (useMiniBloomFilter ? MINI_BLOOM_FILTER_BYTE : 0));

    }

    public static double getHashtableOverheadRatio(int tableSize, int frameSize, int framesLimit, int recordSizeInByte) {
        int pagesForRecord = framesLimit - getHeaderPages(tableSize, frameSize);
        int recordsInHashtable = (pagesForRecord - 1) * ((int) (frameSize / (recordSizeInByte + 2 * INT_SIZE)));

        return (double) framesLimit * frameSize / recordsInHashtable / recordSizeInByte;
    }

    public static int getHeaderPages(int tableSize, int frameSize) {
        int slotsPerPage = frameSize / (INT_SIZE * 2 + MINI_BLOOM_FILTER_BYTE);
        return (int) Math.ceil((double) tableSize / slotsPerPage);
    }

    private int getHeaderPagesConsiderMiniBF(int tableSize, int frameSize) {
        return (int) Math.ceil(tableSize / headerEntryPerFrame);
    }

    private void add(int h, int headerFrameIndex, int headerFrameOffset, boolean isInitialize) {
        int byteIndex = headerFrameOffset + 2 * INT_SIZE;
        if (isInitialize) {
            headers[headerFrameIndex].put(byteIndex, (byte) 0);
        }
        for (int i = 0; i < PRIME_FUNC_COUNT; i++) {
            int bitIndex = (int) (h >> (12 * i)) & 0x07;
            headers[headerFrameIndex].put(byteIndex,
                    (byte) (headers[headerFrameIndex].get(byteIndex) | (1 << bitIndex)));
        }
    }

    private boolean lookup(int h, int headerFrameIndex, int headerFrameOffset) {
        int byteIndex = headerFrameOffset + 2 * INT_SIZE;
        for (int i = 0; i < PRIME_FUNC_COUNT; i++) {
            int bitIndex = (int) (h >> (12 * i)) & 0x07;
            if (!((headers[headerFrameIndex].get(byteIndex) & (1L << bitIndex)) != 0)) {
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        // initialize hash headers
        int htHeaderCount = getHeaderPagesConsiderMiniBF(tableSize, frameSize);

        isPartitionOnly = false;
        if (numOfPartitions >= framesLimit - htHeaderCount) {
            isPartitionOnly = true;
        }

        if (isPartitionOnly) {
            htHeaderCount = 0;
        }

        headers = new ByteBuffer[htHeaderCount];
        for (int i = 0; i < headers.length; i++) {
            headers[i] = ctx.allocateFrame();
            resetHeader(i);
        }

        // initialize hash table contents
        contents = new ByteBuffer[framesLimit - htHeaderCount - numOfPartitions];
        currentHashTableFrame = 0;
        isHashtableFull = false;

        // initialize hash table aggregate state
        htAggregateState = aggregator.createAggregateStates();

        // initialize partition information
        spilledPartOutputBuffers = new ByteBuffer[numOfPartitions];
        spilledPartRunWriters = new RunFileWriter[numOfPartitions];
        spilledPartRunSizeArrayInFrames = new int[numOfPartitions];
        spilledPartRunSizeArrayInTuples = new int[numOfPartitions];

        // initialize other helper classes
        inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inputRecordDescriptor);
        internalTupleBuilder = new ArrayTupleBuilder(outputRecordDescriptor.getFieldCount());
        spilledPartInsertAppender = new FrameTupleAppender(frameSize);

        htInsertAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
        matchPointer = new int[2];
        hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outputRecordDescriptor);

    }

    /**
     * Get the header frame index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderFrameIndex(int entry) {
        return entry / headerEntryPerFrame;
    }

    /**
     * Get the tuple index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderTupleIndex(int entry) {
        return entry % headerEntryPerFrame * (2 * INT_SIZE + (useMiniBloomFilter ? MINI_BLOOM_FILTER_BYTE : 0));
    }

    /**
     * reset the header page.
     * 
     * @param headerFrameIndex
     */
    private void resetHeader(int headerFrameIndex) {
        for (int i = 0; i < frameSize; i += INT_SIZE) {
            headers[headerFrameIndex].putInt(i, HEADER_REF_EMPTY);
        }
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inputFrameTupleAccessor.reset(buffer);
        int tupleCount = inputFrameTupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            insert(inputFrameTupleAccessor, i);
        }
    }

    private void insert(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {

        int hash = hashComputer.partition(accessor, tupleIndex, Integer.MAX_VALUE);

        if (isPartitionOnly) {
            // for partition only
            int pid = hash % numOfPartitions;
            insertSpilledPartition(accessor, tupleIndex, pid);
            spilledPartRunSizeArrayInTuples[pid]++;
            return;
        }

        int entry = hash % tableSize;

        if (findMatch(entry, hash, accessor, tupleIndex)) {
            // found a matching: do aggregation
            hashtableRecordAccessor.reset(contents[matchPointer[0]]);
            aggregator.aggregate(accessor, tupleIndex, contents[matchPointer[0]].array(), matchPointer[1],
                    tupleAccessHelper.getTupleLength(contents[matchPointer[0]], matchPointer[1]), htAggregateState);
            hashedRawRecords++;
        } else {
            if (isHashtableFull) {
                // when hash table is full: spill the record
                int pid = entry % numOfPartitions;
                insertSpilledPartition(accessor, tupleIndex, pid);
                spilledPartRunSizeArrayInTuples[pid]++;
            } else {
                // insert a new entry into the hash table
                internalTupleBuilder.reset();
                for (int k = 0; k < inputKeys.length; k++) {
                    internalTupleBuilder.addField(accessor, tupleIndex, inputKeys[k]);
                }

                aggregator.init(internalTupleBuilder, accessor, tupleIndex, htAggregateState);

                if (contents[currentHashTableFrame] == null) {
                    contents[currentHashTableFrame] = ctx.allocateFrame();
                }

                int headerFrameIndex = getHeaderFrameIndex(entry);
                int headerFrameOffset = getHeaderTupleIndex(entry);
                int nextFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
                int nextTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

                htInsertAppender.reset(contents[currentHashTableFrame], false);
                if (!htInsertAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize(), nextFrameIndex,
                        nextTupleIndex)) {
                    // hash table is full: try to allocate more frame
                    currentHashTableFrame++;
                    if (currentHashTableFrame >= contents.length) {
                        // no more frame to allocate: stop expending the hash table
                        isHashtableFull = true;

                        // reinsert the record
                        insert(accessor, tupleIndex);

                        return;
                    } else {
                        if (contents[currentHashTableFrame] == null) {
                            contents[currentHashTableFrame] = ctx.allocateFrame();
                        }

                        htInsertAppender.reset(contents[currentHashTableFrame], true);

                        if (!htInsertAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize(), nextFrameIndex,
                                nextTupleIndex)) {
                            throw new HyracksDataException(
                                    "Failed to insert an aggregation partial result into the in-memory hash table: it has the length of "
                                            + internalTupleBuilder.getSize() + " and fields "
                                            + internalTupleBuilder.getFieldEndOffsets().length);
                        }

                    }
                }

                // update hash table reference
                headers[headerFrameIndex].putInt(headerFrameOffset, currentHashTableFrame);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE,
                        getTupleStartOffset(currentHashTableFrame, htInsertAppender.getTupleCount() - 1));

                // update mini-bloom-filter
                if (useMiniBloomFilter) {
                    add(hash, headerFrameIndex, headerFrameOffset, (nextFrameIndex < 0));
                }

                hashedKeys++;
                hashedRawRecords++;
            }
        }
    }

    /**
     * Get the tuple start offset for the given frame and tuple index.
     * 
     * @param frameIndex
     * @param tupleIndex
     * @return
     */
    private int getTupleStartOffset(int frameIndex, int tupleIndex) {
        if (tupleIndex == 0) {
            return 0;
        } else {
            return contents[frameIndex].getInt(frameSize - INT_SIZE - tupleIndex * INT_SIZE);
        }
    }

    /**
     * Insert record into a spilled partition, by directly copying the tuple into the output buffer.
     * 
     * @param accessor
     * @param tupleIndex
     * @param pid
     */
    private void insertSpilledPartition(FrameTupleAccessor accessor, int tupleIndex, int pid)
            throws HyracksDataException {

        if (spilledPartOutputBuffers[pid] == null) {
            spilledPartOutputBuffers[pid] = ctx.allocateFrame();
        }

        spilledPartInsertAppender.reset(spilledPartOutputBuffers[pid], false);

        if (!spilledPartInsertAppender.append(accessor, tupleIndex)) {
            // the output buffer is full: flush
            flushSpilledPartitionOutputBuffer(pid);
            // reset the output buffer
            spilledPartInsertAppender.reset(spilledPartOutputBuffers[pid], true);

            if (!spilledPartInsertAppender.append(accessor, tupleIndex)) {
                throw new HyracksDataException("Failed to insert a record into a spilled partition!");
            }
        }

    }

    /**
     * Flush a spilled partition's output buffer.
     * 
     * @param pid
     * @throws HyracksDataException
     */
    private void flushSpilledPartitionOutputBuffer(int pid) throws HyracksDataException {
        if (spilledPartRunWriters[pid] == null) {
            spilledPartRunWriters[pid] = new RunFileWriter(
                    ctx.createManagedWorkspaceFile("HashHashPrePartitionHashTable"), ctx.getIOManager());
            spilledPartRunWriters[pid].open();
        }

        FrameUtils.flushFrame(spilledPartOutputBuffers[pid], spilledPartRunWriters[pid]);

        spilledPartRunSizeArrayInFrames[pid]++;
    }

    /**
     * Hash table lookup
     * 
     * @param hid
     * @param accessor
     * @param tupleIndex
     * @return
     */
    private boolean findMatch(int hid, int bfhid, FrameTupleAccessor accessor, int tupleIndex) {

        int tupleStartOffset = accessor.getTupleStartOffset(tupleIndex);

        matchPointer[0] = -1;
        matchPointer[1] = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(hid);
        int headerFrameOffset = getHeaderTupleIndex(hid);

        if (headers[headerFrameIndex] == null) {
            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleStartOffset = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        if (entryFrameIndex < 0) {
            return false;
        }

        if (useMiniBloomFilter && isHashtableFull && !lookup(bfhid, headerFrameIndex, headerFrameOffset)) {
            return false;
        }

        while (entryFrameIndex >= 0) {
            matchPointer[0] = entryFrameIndex;
            matchPointer[1] = entryTupleStartOffset;
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            if (compare(accessor, tupleStartOffset, hashtableRecordAccessor, entryTupleStartOffset) == 0) {
                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = tupleAccessHelper.getTupleEndOffset(contents[entryFrameIndex], entryTupleStartOffset);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleStartOffset = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
        }

        return false;
    }

    public void finishup() throws HyracksDataException {
        // spill all output buffers
        ByteBuffer outputBuffer = null;
        for (int i = 0; i < numOfPartitions; i++) {
            if (spilledPartOutputBuffers[i] != null) {
                flushSpilledPartitionOutputBuffer(i);
                if (outputBuffer == null) {
                    outputBuffer = spilledPartOutputBuffers[i];
                } else {
                    spilledPartOutputBuffers[i] = null;
                }
            }
        }

        if (outputBuffer == null) {
            outputBuffer = ctx.allocateFrame();
        }

        // flush in-memory aggregation results: no more frame cost here as all output buffers are recycled
        FrameTupleAppender outputBufferAppender = new FrameTupleAppender(frameSize);
        outputBufferAppender.reset(outputBuffer, true);

        ArrayTupleBuilder outFlushTupleBuilder = new ArrayTupleBuilder(outputRecordDescriptor.getFieldCount());

        for (ByteBuffer htFrame : contents) {
            if (htFrame == null) {
                continue;
            }
            hashtableRecordAccessor.reset(htFrame);
            int tupleCount = hashtableRecordAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                outFlushTupleBuilder.reset();

                for (int k = 0; k < internalKeys.length; k++) {
                    outFlushTupleBuilder.addField(hashtableRecordAccessor, i, internalKeys[k]);
                }

                aggregator.outputFinalResult(outFlushTupleBuilder, hashtableRecordAccessor, i, htAggregateState);

                if (!outputBufferAppender.append(outFlushTupleBuilder.getFieldEndOffsets(),
                        outFlushTupleBuilder.getByteArray(), 0, outFlushTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    outputBufferAppender.reset(outputBuffer, true);

                    if (!outputBufferAppender.append(outFlushTupleBuilder.getFieldEndOffsets(),
                            outFlushTupleBuilder.getByteArray(), 0, outFlushTupleBuilder.getSize())) {
                        throw new HyracksDataException(
                                "Failed to flush a record from in-memory hash table: record has length of "
                                        + outFlushTupleBuilder.getSize() + " and fields "
                                        + outFlushTupleBuilder.getFieldEndOffsets().length);
                    }
                }
            }
        }

        if (outputBufferAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
        }

        // create run readers and statistic information for spilled runs
        spilledPartRunReaders = new LinkedList<IFrameReader>();
        spilledRunAggregatedPages = new LinkedList<Integer>();
        spilledPartRunSizesInFrames = new LinkedList<Integer>();
        spilledPartRunSizesInTuples = new LinkedList<Integer>();
        for (int i = 0; i < numOfPartitions; i++) {
            if (spilledPartRunWriters[i] != null) {
                spilledPartRunReaders.add(spilledPartRunWriters[i].createReader());
                spilledRunAggregatedPages.add(0);
                spilledPartRunWriters[i].close();
                spilledPartRunSizesInFrames.add(spilledPartRunSizeArrayInFrames[i]);
                spilledPartRunSizesInTuples.add(spilledPartRunSizeArrayInTuples[i]);
            }
        }
    }

    /**
     * Compare an input record with a hash table entry.
     * 
     * @param accessor
     * @param tupleIndex
     * @param hashAccessor
     * @param hashTupleIndex
     * @return
     */
    private int compare(FrameTupleAccessor accessor, int tupleStartOffset,
            FrameTupleAccessorForGroupHashtable hashAccessor, int hashTupleStartOffset) {

        int fStartOffset0 = accessor.getFieldSlotsLength() + tupleStartOffset;

        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + hashTupleStartOffset;

        for (int i = 0; i < inputKeys.length; ++i) {
            int fStart0 = inputKeys[i] == 0 ? 0 : accessor.getBuffer()
                    .getInt(tupleStartOffset + (inputKeys[i] - 1) * 4);
            int fEnd0 = accessor.getBuffer().getInt(tupleStartOffset + inputKeys[i] * 4);
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

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#close()
     */
    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < numOfPartitions; i++) {
            if (spilledPartRunWriters[i] != null) {
                spilledPartRunWriters[i].close();
            }
        }
        htAggregateState.close();
        contents = null;
        headers = null;
    }

    public List<Integer> getSpilledRunsSizeInRawTuples() throws HyracksDataException {
        return spilledPartRunSizesInTuples;
    }

    public int getHashedUniqueKeys() throws HyracksDataException {
        return hashedKeys;
    }

    public int getHashedRawRecords() throws HyracksDataException {
        return hashedRawRecords;
    }

    public List<Integer> getSpilledRunsAggregatedPages() throws HyracksDataException {
        return spilledRunAggregatedPages;
    }

    public List<IFrameReader> getSpilledRuns() throws HyracksDataException {
        return spilledPartRunReaders;
    }

    public List<Integer> getSpilledRunsSizeInPages() throws HyracksDataException {
        return spilledPartRunSizesInFrames;
    }

}
