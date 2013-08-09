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

public class FrameSortGrouper {

    private static final int POINTER_LENGTH = 3;
    private static final int INT_SIZE = 4;

    private final IAggregatorDescriptor aggregator;
    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    private final INormalizedKeyComputer nkc;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor outRecordDesc;

    protected final List<ByteBuffer> buffers;
    protected final FrameTupleAccessor fta1;
    private final FrameTupleAccessor fta2;

    private ByteBuffer outFrame;
    private ArrayTupleBuilder tupleBuilder;
    private FrameTupleAppender appender;

    private int dataFrameCount;
    protected int[] tPointers;
    private int[] tPointersTemp;
    protected int tupleCount;

    private AggregateState aggregateState;
    private byte[] groupResultCache;
    private ByteBuffer groupResultCacheBuffer;
    private FrameTupleAppender groupResultCacheAppender;
    private FrameTupleAccessor groupResultCacheAccessor;

    public FrameSortGrouper(IHyracksTaskContext ctx, int[] keyFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptor aggregator, RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor)
            throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.nkc = (firstKeyNormalizerFactory == null) ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        this.outRecordDesc = outRecordDescriptor;
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.aggregator = aggregator;
        this.buffers = new ArrayList<ByteBuffer>();
        this.fta1 = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDescriptor);
        this.fta2 = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDescriptor);

        this.aggregateState = this.aggregator.createAggregateStates();

        this.outFrame = ctx.allocateFrame();
        this.tupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFieldCount());

        this.dataFrameCount = 0;
        this.tupleCount = 0;
    }

    public void reset() {
        dataFrameCount = 0;
        tupleCount = 0;
    }

    public int getFrameCount() {
        return dataFrameCount;
    }

    public void insertFrame(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer copyFrame;
        if (dataFrameCount == buffers.size()) {
            copyFrame = ctx.allocateFrame();
            buffers.add(copyFrame);
        } else {
            copyFrame = buffers.get(dataFrameCount);
        }
        FrameUtils.copy(buffer, copyFrame);
        ++dataFrameCount;
    }

    public void sortFrames() {
        int nBuffers = dataFrameCount;
        tupleCount = 0;
        for (int i = 0; i < nBuffers; ++i) {
            fta1.reset(buffers.get(i));
            tupleCount += fta1.getTupleCount();
        }
        int sfIdx = keyFields[0];
        tPointers = tPointers == null || tPointers.length < tupleCount * POINTER_LENGTH ? new int[tupleCount
                * POINTER_LENGTH] : tPointers;
        int ptr = 0;
        for (int i = 0; i < nBuffers; ++i) {
            fta1.reset(buffers.get(i));
            int tCount = fta1.getTupleCount();
            byte[] array = fta1.getBuffer().array();
            for (int j = 0; j < tCount; ++j) {

                tPointers[ptr * POINTER_LENGTH] = i;
                tPointers[ptr * POINTER_LENGTH + 1] = j;
                int tStart = fta1.getTupleStartOffset(j);
                int f0StartRel = fta1.getFieldStartOffset(j, sfIdx);
                int f0EndRel = fta1.getFieldEndOffset(j, sfIdx);
                int f0Start = f0StartRel + tStart + fta1.getFieldSlotsLength();
                tPointers[ptr * POINTER_LENGTH + 2] = nkc == null ? 0 : nkc.normalize(array, f0Start, f0EndRel
                        - f0StartRel);
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
        fta1.reset(buffers.get(buf1));
        fta2.reset(buffers.get(buf2));
        byte[] b1 = fta1.getBuffer().array();
        byte[] b2 = fta2.getBuffer().array();
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = keyFields[f];
            int s1 = fta1.getTupleStartOffset(tid1) + fta1.getFieldStartOffset(tid1, fIdx);
            int l1 = fta1.getFieldLength(tid1, fIdx);

            int s2 = fta2.getTupleStartOffset(tid2) + fta2.getFieldStartOffset(tid2, fIdx);
            int l2 = fta2.getFieldLength(tid2, fIdx);
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private void copy(int src, int dest) {
        for (int i = 0; i < POINTER_LENGTH; i++) {
            tPointersTemp[dest * POINTER_LENGTH + i] = tPointers[src * POINTER_LENGTH + i];
        }
    }

    public void flushFrames(IFrameWriter writer) throws HyracksDataException {

        if (tupleCount <= 0) {
            return;
        }

        for (int ptr = 0; ptr < tupleCount; ptr++) {
            int bufIdx = tPointers[ptr * POINTER_LENGTH];
            int tupleIdx = tPointers[ptr * POINTER_LENGTH + 1];
            fta1.reset(buffers.get(bufIdx));
            if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {

                if (compare(ptr, ptr - 1) == 0) {
                    // could be aggregated: do aggregation and move to the next tuple
                    aggregator.aggregate(fta1, tupleIdx, groupResultCacheAccessor.getBuffer().array(), 0,
                            groupResultCacheAccessor.getTupleStartOffset(0), aggregateState);
                    continue;
                } else {
                    writeOutput(groupResultCacheAccessor, 0, writer);
                }
            }

            tupleBuilder.reset();
            fta1.reset(buffers.get(tPointers[0]));
            for (int i : keyFields) {
                tupleBuilder.addField(fta1, tPointers[1], i);
            }
            aggregator.init(tupleBuilder, fta1, tPointers[1], aggregateState);

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

        tupleBuilder.reset();
        for (int j = 0; j < keyFields.length; j++) {
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
}
