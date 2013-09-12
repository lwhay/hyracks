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
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.HistogramUtils;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper;

/**
 * An implementation of aggregating each frame of the input data using sort-based approach.
 */
public class SortGrouper implements IPushBasedGrouper {

    private static final int POINTER_LENGTH = 3;
    private static final int INT_SIZE = 4;

    private final IAggregatorDescriptor aggregator;
    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    protected final int[] decorFields;
    private final int framesLimit;
    private final INormalizedKeyComputer nkc;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor outRecordDesc;

    protected final List<ByteBuffer> buffers;
    protected final FrameTupleAccessor bufferTupleAccessor;
    private final FrameTupleAccessor bufferTupleAccessorForComparison;

    private ByteBuffer outFrame;
    private ArrayTupleBuilder tupleBuilder;
    private FrameTupleAppender appender;

    private int dataFrameCount;
    protected int[] tPointers;
    private int[] tPointersTemp;
    protected int tupleCount;

    private int[] histogram;
    private boolean enableHistogram = false;

    private AggregateState aggregateState;
    private byte[] groupResultCache;
    private ByteBuffer groupResultCacheBuffer;
    private FrameTupleAppender groupResultCacheAppender;
    private FrameTupleAccessor groupResultCacheAccessor;

    public SortGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.nkc = (firstKeyNormalizerFactory == null) ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        this.outRecordDesc = outRecordDescriptor;
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDescriptor, outRecordDescriptor, keyFields,
                keyFields);
        this.buffers = new ArrayList<ByteBuffer>();
        this.bufferTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDescriptor);
        this.bufferTupleAccessorForComparison = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDescriptor);
        this.histogram = new int[HistogramUtils.HISTOGRAM_SLOTS];

    }

    public void init() throws HyracksDataException {

        this.aggregateState = this.aggregator.createAggregateStates();

        this.outFrame = ctx.allocateFrame();
        this.tupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFieldCount());
        this.dataFrameCount = 0;
        this.tupleCount = 0;

        for (int i = 0; i < this.histogram.length; i++) {
            histogram[i] = 0;
        }
    }

    public void reset() throws HyracksDataException {
        this.dataFrameCount = 0;
        this.tupleCount = 0;
        for (int i = 0; i < histogram.length; i++) {
            histogram[i] = 0;
        }
        this.aggregateState.reset();
        this.tupleBuilder.reset();
    }

    public int getFrameCount() {
        return dataFrameCount;
    }

    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer copyFrame;
        if (dataFrameCount == buffers.size()) {
            if (dataFrameCount < framesLimit) {
                copyFrame = ctx.allocateFrame();
                buffers.add(copyFrame);
            } else {
                return false;
            }
        } else {
            copyFrame = buffers.get(dataFrameCount);
        }
        FrameUtils.copy(buffer, copyFrame);
        ++dataFrameCount;
        return true;
    }

    private void sortFrames() throws HyracksDataException {
        int nBuffers = dataFrameCount;
        tupleCount = 0;
        for (int i = 0; i < nBuffers; ++i) {
            bufferTupleAccessor.reset(buffers.get(i));
            tupleCount += bufferTupleAccessor.getTupleCount();
        }

        tPointers = (tPointers == null || tPointers.length < tupleCount * POINTER_LENGTH) ? new int[tupleCount
                * POINTER_LENGTH] : tPointers;
        int ptr = 0;
        for (int i = 0; i < nBuffers; ++i) {
            bufferTupleAccessor.reset(buffers.get(i));
            int tCount = bufferTupleAccessor.getTupleCount();
            byte[] array = bufferTupleAccessor.getBuffer().array();
            for (int j = 0; j < tCount; ++j) {

                // do histogram update if needed
                if (enableHistogram) {
                    histogram[HistogramUtils.getHistogramBucketID(bufferTupleAccessor, j, keyFields)]++;
                }

                tPointers[ptr * POINTER_LENGTH] = i;
                tPointers[ptr * POINTER_LENGTH + 1] = j;
                if (keyFields.length > 0 && nkc != null) {
                    int tStart = bufferTupleAccessor.getTupleStartOffset(j);
                    int f0StartRel = bufferTupleAccessor.getFieldStartOffset(j, keyFields[0]);
                    int f0EndRel = bufferTupleAccessor.getFieldEndOffset(j, keyFields[0]);
                    int f0Start = f0StartRel + tStart + bufferTupleAccessor.getFieldSlotsLength();
                    tPointers[ptr * POINTER_LENGTH + 2] = nkc.normalize(array, f0Start, f0EndRel - f0StartRel);
                } else {
                    tPointers[ptr * POINTER_LENGTH + 2] = 0;
                }
                ++ptr;
            }
        }
        if (tupleCount > 0) {
            tPointersTemp = new int[tPointers.length];
            sort(0, tupleCount);
        }
    }

    private void sort(int offset, int length) {
        int step = 1;
        int len = length;
        int end = offset + len;
        /** bottom-up merge */
        while (step < len) {
            /** merge */
            for (int i = offset; i < end; i += 2 * step) {
                int next = i + step;
                if (next < end) {
                    merge(i, next, step, Math.min(step, end - next));
                } else {
                    System.arraycopy(tPointers, i * POINTER_LENGTH, tPointersTemp, i * POINTER_LENGTH, (end - i)
                            * POINTER_LENGTH);
                }
            }
            /** prepare next phase merge */
            step *= 2;
            int[] tmp = tPointersTemp;
            tPointersTemp = tPointers;
            tPointers = tmp;
        }
    }

    /** Merge two subarrays into one */
    private void merge(int start1, int start2, int len1, int len2) {
        int targetPos = start1;
        int pos1 = start1;
        int pos2 = start2;
        int end1 = start1 + len1 - 1;
        int end2 = start2 + len2 - 1;
        while (pos1 <= end1 && pos2 <= end2) {
            int cmp = compare(pos1, pos2);
            if (cmp <= 0) {
                copy(pos1, targetPos);
                pos1++;
            } else {
                copy(pos2, targetPos);
                pos2++;
            }
            targetPos++;
        }
        if (pos1 <= end1) {
            int rest = end1 - pos1 + 1;
            System.arraycopy(tPointers, pos1 * POINTER_LENGTH, tPointersTemp, targetPos * POINTER_LENGTH, rest
                    * POINTER_LENGTH);
        }
        if (pos2 <= end2) {
            int rest = end2 - pos2 + 1;
            System.arraycopy(tPointers, pos2 * POINTER_LENGTH, tPointersTemp, targetPos * POINTER_LENGTH, rest
                    * POINTER_LENGTH);
        }
    }

    protected int compare(int tp1, int tp2) {

        int buf1 = tPointers[tp1 * POINTER_LENGTH];
        int tid1 = tPointers[tp1 * POINTER_LENGTH + 1];
        int nk1 = tPointers[tp1 * POINTER_LENGTH + 2];

        int buf2 = tPointers[tp2 * POINTER_LENGTH];
        int tid2 = tPointers[tp2 * POINTER_LENGTH + 1];
        int nk2 = tPointers[tp2 * POINTER_LENGTH + 2];

        if (nk1 != nk2) {
            return ((((long) nk1) & 0xffffffffL) < (((long) nk2) & 0xffffffffL)) ? -1 : 1;
        }
        bufferTupleAccessor.reset(buffers.get(buf1));
        bufferTupleAccessorForComparison.reset(buffers.get(buf2));
        byte[] b1 = bufferTupleAccessor.getBuffer().array();
        byte[] b2 = bufferTupleAccessorForComparison.getBuffer().array();
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = keyFields[f];
            int s1 = bufferTupleAccessor.getTupleStartOffset(tid1)
                    + bufferTupleAccessor.getFieldStartOffset(tid1, fIdx);
            int l1 = bufferTupleAccessor.getFieldLength(tid1, fIdx);

            int s2 = bufferTupleAccessorForComparison.getTupleStartOffset(tid2)
                    + bufferTupleAccessorForComparison.getFieldStartOffset(tid2, fIdx);
            int l2 = bufferTupleAccessorForComparison.getFieldLength(tid2, fIdx);
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    protected boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
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

    private void copy(int src, int dest) {
        for (int i = 0; i < POINTER_LENGTH; i++) {
            tPointersTemp[dest * POINTER_LENGTH + i] = tPointers[src * POINTER_LENGTH + i];
        }
    }

    public void flush(IFrameWriter writer) throws HyracksDataException {

        // sort the data before flushing
        sortFrames();

        if (tupleCount <= 0) {
            return;
        }

        for (int ptr = 0; ptr < tupleCount; ptr++) {
            int bufIdx = tPointers[ptr * POINTER_LENGTH];
            int tupleIdx = tPointers[ptr * POINTER_LENGTH + 1];
            bufferTupleAccessor.reset(buffers.get(bufIdx));
            if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {

                groupResultCacheAccessor.reset(ByteBuffer.wrap(groupResultCache));

                if (sameGroup(bufferTupleAccessor, tupleIdx, groupResultCacheAccessor, 0)) {
                    // find match: do aggregation
                    int groupCacheStartOffset = groupResultCacheAccessor.getTupleStartOffset(0);
                    aggregator.aggregate(bufferTupleAccessor, tupleIdx, groupResultCacheAccessor.getBuffer().array(),
                            groupCacheStartOffset, groupResultCacheAccessor.getTupleEndOffset(0)
                                    - groupCacheStartOffset, aggregateState);
                    continue;
                } else {
                    // write the cached group into the final output
                    writeOutput(groupResultCacheAccessor, 0, writer);
                }
            }

            tupleBuilder.reset();
            bufferTupleAccessor.reset(buffers.get(tPointers[0]));
            for (int i : keyFields) {
                tupleBuilder.addField(bufferTupleAccessor, tPointers[1], i);
            }
            for (int i : decorFields) {
                tupleBuilder.addField(bufferTupleAccessor, tPointers[1], i);
            }
            aggregator.init(tupleBuilder, bufferTupleAccessor, tPointers[1], aggregateState);

            // enlarge the cache buffer if needed
            int requiredSize = tupleBuilder.getSize() + tupleBuilder.getFieldEndOffsets().length * INT_SIZE + 2
                    * INT_SIZE;

            if (groupResultCache == null || groupResultCache.length < requiredSize) {
                groupResultCache = new byte[requiredSize];
                groupResultCacheAppender = new FrameTupleAppender(groupResultCache.length);
                groupResultCacheBuffer = ByteBuffer.wrap(groupResultCache);
                groupResultCacheAccessor = new FrameTupleAccessor(groupResultCache.length, outRecordDesc);
            }

            groupResultCacheAppender.reset(groupResultCacheBuffer, true);
            if (!groupResultCacheAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("The partial result is too large to be initialized in a frame.");
            }

            groupResultCacheAccessor.reset(groupResultCacheBuffer);
        }

        if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {
            writeOutput(groupResultCacheAccessor, 0, writer);
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(outFrame, writer);
            }
        }
    }

    public void close() {
        this.buffers.clear();
        aggregateState.close();
        this.outFrame = null;
    }

    private void writeOutput(FrameTupleAccessor lastTupleAccessor, int lastTupleIndex, IFrameWriter writer)
            throws HyracksDataException {

        if (appender == null) {
            appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(outFrame, true);
        }

        tupleBuilder.reset();
        for (int j = 0; j < keyFields.length + decorFields.length; j++) {
            tupleBuilder.addField(lastTupleAccessor, lastTupleIndex, j);
        }
        aggregator.outputFinalResult(tupleBuilder, lastTupleAccessor, lastTupleIndex, aggregateState);

        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
            FrameUtils.flushFrame(outFrame, writer);
            appender.reset(outFrame, true);
            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }

    }

    @Override
    public int[] getDataDistHistogram() throws HyracksDataException {
        return histogram;
    }
}
