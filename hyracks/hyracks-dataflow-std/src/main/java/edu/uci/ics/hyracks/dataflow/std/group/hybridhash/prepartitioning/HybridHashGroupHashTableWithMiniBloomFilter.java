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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.prepartitioning;

import java.nio.ByteBuffer;
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
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.FrameTupleAccessorForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.FrameTupleAppenderForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.TupleAccessHelper;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HybridHashGroupHashTableWithMiniBloomFilter implements IFrameWriter {

    private static final int HEADER_REF_EMPTY = -1;

    private static final int INT_SIZE = 4;

    private static final int MINI_BLOOM_FILTER_BYTE = 1;

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
    

    public HybridHashGroupHashTableWithMiniBloomFilter(IHyracksTaskContext ctx, int framesLimit, int tableSize,
            int numOfPartitions, int[] keys, int hashSeedOffset, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily tpcFamily, IAggregatorDescriptor aggregator,
            RecordDescriptor inputRecordDescriptor, RecordDescriptor outputRecordDescriptor, IFrameWriter outputWriter)
            throws HyracksDataException {
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
    }

    public static double getHashtableOverheadRatio(int tableSize, int frameSize, int framesLimit, int recordSizeInByte) {
        int pagesForRecord = framesLimit - getHeaderPages(tableSize, frameSize);
        int recordsInHashtable = (pagesForRecord - 1) * ((int) (frameSize / (recordSizeInByte + 2 * INT_SIZE)));

        return (double) framesLimit * frameSize / recordsInHashtable / recordSizeInByte;
    }

    public static int getHeaderPages(int tableSize, int frameSize) {
        return (int) Math.ceil((double) tableSize * (INT_SIZE * 2 + MINI_BLOOM_FILTER_BYTE) / frameSize);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        // initialize hash headers
        int htHeaderCount = getHeaderPages(tableSize, frameSize);

        isPartitionOnly = false;
        if (numOfPartitions >= framesLimit - htHeaderCount) {
            isPartitionOnly = true;
        }

        if (isPartitionOnly) {
            htHeaderCount = 0;
        }

        headers = new ByteBuffer[htHeaderCount];
        for (int i = 0; i < headers.length; i++) {
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
        int frameIndex = (entry / frameSize * 2 * INT_SIZE) + (entry % frameSize * 2 * INT_SIZE / frameSize);
        return frameIndex;
    }

    /**
     * Get the tuple index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderTupleIndex(int entry) {
        int offset = (entry % frameSize) * 2 * INT_SIZE % frameSize;
        return offset;
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
        // TODO Auto-generated method stub

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
        // TODO Auto-generated method stub

    }

}
