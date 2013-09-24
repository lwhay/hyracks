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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.GrouperFlushOption;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IGrouperFlushOption;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IGrouperFlushOption.GroupOutputState;

/**
 * A hybrid-hash grouper groups records so that an in-memory hash table is built to complete the aggregation
 * for a portion of the input records (called the resident partition), while the rest of the records are spilled
 * into the corresponding writers for either disk or network dumpping.
 */
public class HybridHashGrouper extends AbstractHistogramPushBasedGrouper {

    protected static final int LIST_FRAME_REF_SIZE = 4;
    protected static final int LIST_TUPLE_REF_SIZE = 4;
    protected static final int POINTER_INIT_SIZE = 8;
    protected static final int POINTER_LENGTH = 3;

    protected static final int BLOOM_FILTER_SIZE = 1;
    protected static final int PRIME_FUNC_COUNT = 3;

    protected final int tableSize;

    private final IAggregatorDescriptor aggregator, merger;
    private AggregateState aggState;

    private final IBinaryComparator[] comparators;

    private ITuplePartitionComputer tuplePartitionComputer;

    protected ByteBuffer[] headers;
    protected ByteBuffer[] contents;
    protected ByteBuffer[] spilledPartitionBuffers;

    protected RunFileWriter[] spillingPartitionRunWriters;

    private final boolean useBloomFilter;

    private final int partitions;

    private ByteBuffer outputBuffer;

    private FrameTupleAccessor inputFrameTupleAccessor, groupFrameAccessor;

    private FrameTupleAppender outputAppender;

    private HashTableFrameTupleAppender hashtableFrameTupleAppender;
    private FrameTupleAppender spillFrameTupleAppender;

    private ArrayTupleBuilder groupTupleBuilder, outputTupleBuilder;

    private int currentWorkingFrame;

    /**
     * For the hash table lookup
     */
    private int lookupFrameIndex, lookupTupleIndex;
    private byte bloomFilterByte;

    private boolean isHashTableFull;

    // counter for the number of tuples that have been processed
    private int processedTuple;

    /**
     * For collecting statistic information
     */
    private int rawRecordsInResidentPartition, groupsInResidentPartition;
    private int[] rawRecordsInSpillingPartitions;

    private List<Integer> recordsInRuns;

    /**
     * For debug
     */
    private long inRecCounter = 0, outRecCounter = 0, hashHitCompCounter = 0, hashMissCompCounter = 0,
            outFrameCounter = 0, compCounter = 0, bloomFilterUpdateCounter = 0, bloomFilterLookupCounter = 0;;
    private int nonEmptySlotCount = 0;

    public HybridHashGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, boolean enableHistorgram,
            IFrameWriter outputWriter, boolean isGenerateRuns, int tableSize,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFactory[] hashFunctionFactories,
            int partitions, boolean useBloomFilter) throws HyracksDataException {
        super(ctx, keyFields, decorFields, framesLimit, aggregatorFactory, mergerFactory, inRecDesc, outRecDesc,
                enableHistorgram, outputWriter, isGenerateRuns);

        this.tableSize = tableSize;

        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < this.comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.tuplePartitionComputer = new FieldHashPartitionComputerFactory(keyFields, hashFunctionFactories)
                .createPartitioner();

        int[] storedKeys = new int[keyFields.length];

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecDesc, outRecDesc, keyFields, storedKeys);
        this.aggState = aggregator.createAggregateStates();
        this.merger = mergerFactory.createAggregator(ctx, outRecDesc, outRecDesc, storedKeys, storedKeys);

        this.useBloomFilter = useBloomFilter;

        this.partitions = partitions;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#init()
     */
    @Override
    public void open() throws HyracksDataException {

        // initialize the hash table
        int headerFramesCount = (int) (Math.ceil((double) tableSize
                * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE + (useBloomFilter ? BLOOM_FILTER_SIZE : 0)) / frameSize));

        if (framesLimit < headerFramesCount + 2) {
            throw new HyracksDataException("Not enough frame (" + framesLimit + ") for a hash table with " + tableSize
                    + " slots.");
        }

        this.headers = new ByteBuffer[headerFramesCount];
        for (int i = 0; i < headers.length; i++) {
            this.headers[i] = ctx.allocateFrame();
        }

        resetHeaders();
        // - list storage area

        if (framesLimit - headers.length - partitions - 1 < 0) {
            throw new HyracksDataException("Note enough memory for the hybrid hash algorithm: " + headers.length
                    + " headers and " + partitions + " partitions.");
        }

        this.contents = new ByteBuffer[framesLimit - headers.length - partitions - 1];
        for (int i = 0; i < contents.length; i++) {
            this.contents[i] = ctx.allocateFrame();
        }

        // initialize the run file writer array
        this.spillingPartitionRunWriters = new RunFileWriter[partitions];

        // initialize the accessors and appenders
        this.inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inRecDesc);
        this.groupFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        this.groupTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());

        this.hashtableFrameTupleAppender = new HashTableFrameTupleAppender(frameSize, LIST_FRAME_REF_SIZE
                + LIST_TUPLE_REF_SIZE);

        // reset the hash table content frame 
        this.currentWorkingFrame = 0;

        // reset the lookup reference
        this.lookupFrameIndex = -1;
        this.lookupTupleIndex = -1;

        resetHistogram();

        this.rawRecordsInResidentPartition = 0;
        this.groupsInResidentPartition = 0;
        this.rawRecordsInSpillingPartitions = new int[partitions];
        this.recordsInRuns = new LinkedList<Integer>();

        this.spilledPartitionBuffers = new ByteBuffer[partitions];

        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
    }

    private void resetHeaders() {
        for (int i = 0; i < headers.length; i++) {
            headers[i].position(0);
            while (headers[i].position() + (useBloomFilter ? 9 : 8) < frameSize) {
                if (useBloomFilter) {
                    headers[i].put((byte) 0);
                }
                headers[i].putInt(-1);
                headers[i].putInt(-1);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#nextFrame(java.nio.ByteBuffer, int)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // reset the processed tuple count
        this.processedTuple = 0;

        inputFrameTupleAccessor.reset(buffer);

        int tupleCount = inputFrameTupleAccessor.getTupleCount();

        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
            int h = tuplePartitionComputer.partition(inputFrameTupleAccessor, tupleIndex, tableSize);

            if (findMatch(inputFrameTupleAccessor, tupleIndex, h)) {
                // match found: do aggregation
                this.groupFrameAccessor.reset(contents[lookupFrameIndex]);
                int tupleStartOffset = this.groupFrameAccessor.getTupleStartOffset(lookupTupleIndex);
                int tupleEndOffset = this.groupFrameAccessor.getTupleEndOffset(lookupTupleIndex);
                this.aggregator.aggregate(inputFrameTupleAccessor, tupleIndex, contents[lookupFrameIndex].array(),
                        tupleStartOffset, tupleEndOffset - tupleStartOffset, aggState);

                rawRecordsInResidentPartition++;
            } else {
                // not found: if the hash table is not full, insert into the hash table

                this.groupTupleBuilder.reset();

                for (int i : keyFields) {
                    groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                for (int i : decorFields) {
                    groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                aggregator.init(groupTupleBuilder, inputFrameTupleAccessor, tupleIndex, aggState);

                if (isHashTableFull) {
                    spillGroup(groupTupleBuilder, h);
                    continue;
                }

                // insert the new group into the beginning of the slot
                getSlotPointer(h);

                hashtableFrameTupleAppender.reset(contents[currentWorkingFrame], false);
                if (!hashtableFrameTupleAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                        groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize(), lookupFrameIndex,
                        lookupTupleIndex)) {
                    currentWorkingFrame++;
                    if (currentWorkingFrame >= contents.length) {
                        // hash table is full
                        spillGroup(groupTupleBuilder, h);
                        continue;
                    }
                    if (contents[currentWorkingFrame] == null) {
                        contents[currentWorkingFrame] = ctx.allocateFrame();
                    }
                    hashtableFrameTupleAppender.reset(contents[currentWorkingFrame], true);
                    if (!hashtableFrameTupleAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                            groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize(), lookupFrameIndex,
                            lookupTupleIndex)) {
                        throw new HyracksDataException(
                                "Failed to insert a group into the hash table: the record is too large.");
                    }
                }

                if (useBloomFilter) {
                    bloomFilterByte = insertIntoBloomFilter(h, bloomFilterByte);
                }

                // reset the header reference
                setSlotPointer(h, bloomFilterByte, currentWorkingFrame, hashtableFrameTupleAppender.getTupleCount() - 1);

                groupsInResidentPartition++;
                rawRecordsInResidentPartition++;
            }

            insertIntoHistogram(h);
            this.processedTuple++;

            inRecCounter++;
        }
    }

    private void spillGroup(ArrayTupleBuilder tb, int hashValue) throws HyracksDataException {
        int partitionToSpill = hashValue % partitions;
        if (spilledPartitionBuffers[partitionToSpill] == null) {
            spilledPartitionBuffers[partitionToSpill] = ctx.allocateFrame();
            spillFrameTupleAppender.reset(spilledPartitionBuffers[partitionToSpill], true);
        }
        spillFrameTupleAppender.reset(spilledPartitionBuffers[partitionToSpill], false);
        if (!spillFrameTupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            if (isGenerateRuns) {
                // the buffer for this spilled partition is full
                if (spillingPartitionRunWriters[partitionToSpill] == null) {
                    spillingPartitionRunWriters[partitionToSpill] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()), ctx.getIOManager());
                    spillingPartitionRunWriters[partitionToSpill].open();
                }
                flush(spillingPartitionRunWriters[partitionToSpill], GrouperFlushOption.FLUSH_FOR_GROUP_STATE,
                        partitionToSpill);
            } else {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, partitionToSpill);
            }
            spillFrameTupleAppender.reset(spilledPartitionBuffers[partitionToSpill], true);
            if (!spillFrameTupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new HyracksDataException("Failed to flush a tuple of a spilling partition");
            }
        }
        rawRecordsInSpillingPartitions[partitionToSpill]++;
    }

    private void setSlotPointer(int h, byte bfByte, int contentFrameIndex, int contentTupleIndex) {
        int slotsPerFrame = frameSize
                / (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE + (useBloomFilter ? BLOOM_FILTER_SIZE : 0));
        int slotFrameIndex = h / slotsPerFrame;
        int slotTupleOffset = h % slotsPerFrame
                * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE + (useBloomFilter ? BLOOM_FILTER_SIZE : 0));

        if (useBloomFilter) {
            headers[slotFrameIndex].put(slotTupleOffset, bfByte);
        }
        headers[slotFrameIndex].putInt(slotTupleOffset + (useBloomFilter ? BLOOM_FILTER_SIZE : 0), contentFrameIndex);
        headers[slotFrameIndex].putInt(slotTupleOffset + (useBloomFilter ? BLOOM_FILTER_SIZE : 0) + INT_SIZE,
                contentTupleIndex);
    }

    private void getSlotPointer(int h) {
        int slotsPerFrame = frameSize
                / (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE + (useBloomFilter ? BLOOM_FILTER_SIZE : 0));
        int slotFrameIndex = h / slotsPerFrame;
        int slotTupleOffset = h % slotsPerFrame
                * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE + (useBloomFilter ? BLOOM_FILTER_SIZE : 0));

        if (useBloomFilter) {
            bloomFilterByte = headers[slotFrameIndex].get(slotTupleOffset);
        }
        lookupFrameIndex = headers[slotFrameIndex].getInt(slotTupleOffset + (useBloomFilter ? BLOOM_FILTER_SIZE : 0));
        lookupTupleIndex = headers[slotFrameIndex].getInt(slotTupleOffset + (useBloomFilter ? BLOOM_FILTER_SIZE : 0)
                + INT_SIZE);
    }

    private byte insertIntoBloomFilter(int h, byte bfByte) {
        byte bfByteAfterInsertion = bfByte;
        for (int i = 0; i < PRIME_FUNC_COUNT; i++) {
            int bitIndex = (int) (h >> (12 * i)) & 0x07;
            bfByteAfterInsertion = (byte) (bfByteAfterInsertion | (1 << bitIndex));
        }
        bloomFilterUpdateCounter++;
        return bfByteAfterInsertion;
    }

    private boolean lookupBloomFilter(int h, byte bfByte) {
        bloomFilterLookupCounter++;
        for (int i = 0; i < PRIME_FUNC_COUNT; i++) {
            int bitIndex = (int) (h >> (12 * i)) & 0x07;
            if (!((bfByte & (1L << bitIndex)) != 0)) {
                return false;
            }
        }
        return true;
    }

    private boolean findMatch(FrameTupleAccessor accessor, int tupleIndex, int hashValue) throws HyracksDataException {
        getSlotPointer(hashValue);

        // do bloom filter lookup, if bloom filter is enabled.
        if (useBloomFilter) {
            if (isHashTableFull && !lookupBloomFilter(hashValue, bloomFilterByte)) {
                return false;
            }
        }

        while (lookupFrameIndex >= 0) {
            groupFrameAccessor.reset(contents[lookupFrameIndex]);
            if (!sameGroup(accessor, tupleIndex, groupFrameAccessor, lookupTupleIndex)) {
                int tupleEndOffset = groupFrameAccessor.getTupleEndOffset(lookupTupleIndex);
                lookupFrameIndex = groupFrameAccessor.getBuffer().getInt(
                        tupleEndOffset - (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE));
                lookupTupleIndex = groupFrameAccessor.getBuffer().getInt(tupleEndOffset - LIST_TUPLE_REF_SIZE);
            } else {
                hashHitCompCounter += compCounter;
                compCounter = 0;
                return true;
            }
        }
        hashMissCompCounter += compCounter;
        compCounter = 0;

        return false;
    }

    protected boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
        compCounter++;
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = keyFields[i];
            int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength() + a1.getFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength() + a2.getFieldStartOffset(t2Idx, i);
            int l2 = a2.getFieldLength(t2Idx, i);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    private void flush(IFrameWriter writer, IGrouperFlushOption flushOption, int partitionIndex)
            throws HyracksDataException {

        outputAppender.reset(outputBuffer, true);

        ByteBuffer bufToFlush = null;

        int hashtableFrameIndex = 0;

        IAggregatorDescriptor aggDesc;

        if (flushOption.getOutputState() == GroupOutputState.GROUP_STATE) {
            aggDesc = aggregator;
        } else if (flushOption.getOutputState() == GroupOutputState.RESULT_STATE) {
            aggDesc = merger;
        } else {
            throw new HyracksDataException("Cannot output " + GroupOutputState.RAW_STATE.name()
                    + " for flushing hybrid hash grouper");
        }

        if (partitionIndex >= 0) {
            bufToFlush = spilledPartitionBuffers[partitionIndex];
        } else {
            bufToFlush = contents[hashtableFrameIndex];
        }

        while (bufToFlush != null) {
            groupFrameAccessor.reset(bufToFlush);
            int tupleCount = groupFrameAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                outputTupleBuilder.reset();
                for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                    outputTupleBuilder.addField(groupFrameAccessor, i, k);
                }
                aggDesc.outputFinalResult(outputTupleBuilder, groupFrameAccessor, i, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, writer);
                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException(
                                "Failed to dump a group from the hash table to a frame: possibly the size of the tuple is too large.");
                    }
                }
            }
            if (partitionIndex < 0) {
                hashtableFrameIndex++;
                if (contents.length > hashtableFrameIndex && currentWorkingFrame >= hashtableFrameIndex) {
                    bufToFlush = contents[hashtableFrameIndex];
                } else {
                    bufToFlush = null;
                }
            } else {
                bufToFlush = null;
            }
        }

        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, writer);
            outputAppender.reset(outputBuffer, true);
        }
    }

    @Override
    public void wrapup() throws HyracksDataException {
        if (outputBuffer == null) {
            outputBuffer = ctx.allocateFrame();
        }

        if (outputAppender == null) {
            outputAppender = new FrameTupleAppender(frameSize);
        }

        for (int i = 0; i < partitions; i++) {
            if (spilledPartitionBuffers[i] == null) {
                continue;
            }
            groupFrameAccessor.reset(spilledPartitionBuffers[i]);
            if (groupFrameAccessor.getTupleCount() == 0) {
                continue;
            }
            if (isGenerateRuns) {
                if (spillingPartitionRunWriters[i] == null) {
                    spillingPartitionRunWriters[i] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()), ctx.getIOManager());
                    spillingPartitionRunWriters[i].open();
                }
                flush(spillingPartitionRunWriters[i], GrouperFlushOption.FLUSH_FOR_GROUP_STATE, i);
            } else {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, i);
            }
        }

        // flush the resident partition
        flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, -1);
    }

    @Override
    protected void flush(IFrameWriter writer, GrouperFlushOption flushOption) throws HyracksDataException {
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#reset()
     */
    @Override
    public void reset() throws HyracksDataException {
        ctx.getCounterContext().getCounter(debugID + ".inputRecords", true).update(inRecCounter);
        ctx.getCounterContext().getCounter(debugID + ".outputRecords", true).update(outRecCounter);
        ctx.getCounterContext().getCounter(debugID + ".comparisons.hashhit", true).update(hashHitCompCounter);
        ctx.getCounterContext().getCounter(debugID + ".comparisons.hashmiss", true).update(hashMissCompCounter);
        ctx.getCounterContext().getCounter(debugID + ".outputFrames", true).update(outFrameCounter);
        ctx.getCounterContext().getCounter(debugID + ".usedSlots", true).update(nonEmptySlotCount);
        ctx.getCounterContext().getCounter(debugID + ".bloomFilterUpdate", true).update(bloomFilterUpdateCounter);
        ctx.getCounterContext().getCounter(debugID + ".bloomFilterLookup", true).update(bloomFilterLookupCounter);

        inRecCounter = 0;
        outRecCounter = 0;
        hashHitCompCounter = 0;
        hashMissCompCounter = 0;
        outFrameCounter = 0;
        compCounter = 0;
        nonEmptySlotCount = 0;
        bloomFilterLookupCounter = 0;
        bloomFilterUpdateCounter = 0;

        rawRecordsInResidentPartition = 0;
        groupsInResidentPartition = 0;
        for (int i = 0; i < rawRecordsInSpillingPartitions.length; i++) {
            rawRecordsInSpillingPartitions[i] = 0;
        }

        // reset the hash table content frame 
        this.currentWorkingFrame = 0;

        // reset the lookup reference
        this.lookupFrameIndex = -1;
        this.lookupTupleIndex = -1;

        // reset header pages
        resetHeaders();

        resetHistogram();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#close()
     */
    @Override
    public void close() throws HyracksDataException {

        // flush the resident partition (all contents in the hash table)

        this.headers = null;
        this.contents = null;
        this.spilledPartitionBuffers = null;
    }

    public int getProcessedTupleCount() {
        return this.processedTuple;
    }

    protected enum HybridHashFlushOption implements IGrouperFlushOption {

        FLUSH_SPILLED_PARTITION_FOR_GROUP_STATE(GroupOutputState.GROUP_STATE),
        FLUSH_SPILLED_PARTITION_FOR_RESULT_STATE(GroupOutputState.RESULT_STATE),
        FLUSH_HASHTABLE_FOR_GROUP_STATE(GroupOutputState.GROUP_STATE),
        FLUSH_HASHTABLE_FOR_RESULT_STATE(GroupOutputState.RESULT_STATE);

        private final GroupOutputState outputState;

        private HybridHashFlushOption(GroupOutputState outputState) {
            this.outputState = outputState;
        }

        @Override
        public GroupOutputState getOutputState() {
            return this.outputState;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    public int getRawRecordsInResidentPartition() {
        return rawRecordsInResidentPartition;
    }

    public int getGroupsInResidentPartition() {
        return groupsInResidentPartition;
    }

    public List<Integer> getRawRecordsInSpillingPartitions() {
        return recordsInRuns;
    }
}
