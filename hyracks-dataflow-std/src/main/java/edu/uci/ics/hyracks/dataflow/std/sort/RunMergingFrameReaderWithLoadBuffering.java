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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class RunMergingFrameReaderWithLoadBuffering implements IFrameReader {
    private final IHyracksTaskContext ctx;
    private final IFrameReader[] runCursors;
    private final List<ByteBuffer> inFrames;
    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDesc;
    private final FrameTupleAppender outFrameAppender;
    private ReferencedPriorityQueue topTuples;
    private int[] tupleIndexes;
    private int[] currentFrameIndexForRuns, bufferedFramesForRuns;
    private FrameTupleAccessor[] tupleAccessors;
    private int framesBuffered;

    // FIXME
    private static final Logger LOGGER = Logger.getLogger(RunMergingFrameReaderWithLoadBuffering.class.getSimpleName());
    long comparisonCounter = 0, readTimer = 0;

    public RunMergingFrameReaderWithLoadBuffering(IHyracksTaskContext ctx, IFrameReader[] runCursors, int framesLimit,
            int[] sortFields, IBinaryComparator[] comparators, RecordDescriptor recordDesc) {
        this.ctx = ctx;
        this.runCursors = runCursors;
        this.inFrames = new ArrayList<ByteBuffer>();
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.recordDesc = recordDesc;
        outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
        while (inFrames.size() + 1 < framesLimit) {
            inFrames.add(ctx.allocateFrame());
        }
    }

    @Override
    public void open() throws HyracksDataException {
        tupleAccessors = new FrameTupleAccessor[runCursors.length];
        currentFrameIndexForRuns = new int[runCursors.length];
        bufferedFramesForRuns = new int[runCursors.length];
        Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
        topTuples = new ReferencedPriorityQueue(ctx.getFrameSize(), recordDesc, runCursors.length, comparator);
        tupleIndexes = new int[runCursors.length];

        framesBuffered = 1;
        if (runCursors.length <= inFrames.size()) {
            framesBuffered = inFrames.size() / runCursors.length;
        }

        for (int i = 0; i < runCursors.length; i++) {
            int runIndex = topTuples.peek().getRunid();
            tupleIndexes[runIndex] = 0;
            runCursors[runIndex].open();
            for (int j = 0; j < framesBuffered; j++) {
                // FIXME
                long timer = System.nanoTime();
                boolean hasFrame = runCursors[runIndex].nextFrame(inFrames.get(runIndex * framesBuffered + j));
                readTimer += System.nanoTime() - timer;
                if (hasFrame) {
                    bufferedFramesForRuns[runIndex]++;
                    if (j == 0) {
                        tupleAccessors[runIndex] = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
                        tupleAccessors[runIndex].reset(inFrames.get(runIndex * framesBuffered + j));
                        setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
                        currentFrameIndexForRuns[runIndex] = runIndex * framesBuffered;
                    }
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        outFrameAppender.reset(buffer, true);
        while (!topTuples.areRunsExhausted()) {
            ReferenceEntry top = topTuples.peek();
            int runIndex = top.getRunid();
            FrameTupleAccessor fta = top.getAccessor();
            int tupleIndex = top.getTupleIndex();

            if (!outFrameAppender.append(fta, tupleIndex)) {
                return true;
            }

            ++tupleIndexes[runIndex];
            setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);

        }

        if (outFrameAppender.getTupleCount() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < runCursors.length; ++i) {
            closeRun(i, runCursors, tupleAccessors);
        }
        LOGGER.warning("PhaseC\t" + readTimer + "\t" + comparisonCounter);
    }

    private void setNextTopTuple(int runIndex, int[] tupleIndexes, IFrameReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples) throws HyracksDataException {
        boolean exists = hasNextTuple(runIndex, tupleIndexes, runCursors, tupleAccessors);
        if (exists) {
            topTuples.popAndReplace(tupleAccessors[runIndex], tupleIndexes[runIndex]);
        } else {
            topTuples.pop();
            closeRun(runIndex, runCursors, tupleAccessors);
        }
    }

    private boolean hasNextTuple(int runIndex, int[] tupleIndexes, IFrameReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors) throws HyracksDataException {
        if (tupleAccessors[runIndex] == null || runCursors[runIndex] == null) {
            return false;
        } else if (tupleIndexes[runIndex] >= tupleAccessors[runIndex].getTupleCount()) {
            if (currentFrameIndexForRuns[runIndex] - runIndex * framesBuffered < bufferedFramesForRuns[runIndex] - 1) {
                currentFrameIndexForRuns[runIndex]++;
            } else {
                bufferedFramesForRuns[runIndex] = 0;
                for (int j = 0; j < framesBuffered; j++) {
                    long timer = System.nanoTime();
                    boolean hasFrame = runCursors[runIndex].nextFrame(inFrames.get(runIndex * framesBuffered + j));
                    readTimer += System.nanoTime() - timer;
                    if (hasFrame) {
                        bufferedFramesForRuns[runIndex]++;
                    } else {
                        break;
                    }
                }
                currentFrameIndexForRuns[runIndex] = runIndex * framesBuffered;
            }
            if (bufferedFramesForRuns[runIndex] > 0) {
                tupleAccessors[runIndex].reset(inFrames.get(currentFrameIndexForRuns[runIndex]));
                tupleIndexes[runIndex] = 0;
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private void closeRun(int index, IFrameReader[] runCursors, IFrameTupleAccessor[] tupleAccessors)
            throws HyracksDataException {
        if (runCursors[index] != null) {
            runCursors[index].close();
            runCursors[index] = null;
            tupleAccessors[index] = null;
        }
    }

    private Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {
        return new Comparator<ReferenceEntry>() {
            public int compare(ReferenceEntry tp1, ReferenceEntry tp2) {
                // FIXME
                comparisonCounter++;
                FrameTupleAccessor fta1 = (FrameTupleAccessor) tp1.getAccessor();
                FrameTupleAccessor fta2 = (FrameTupleAccessor) tp2.getAccessor();
                int j1 = tp1.getTupleIndex();
                int j2 = tp2.getTupleIndex();
                byte[] b1 = fta1.getBuffer().array();
                byte[] b2 = fta2.getBuffer().array();
                for (int f = 0; f < sortFields.length; ++f) {
                    int fIdx = sortFields[f];
                    int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength()
                            + fta1.getFieldStartOffset(j1, fIdx);
                    int l1 = fta1.getFieldEndOffset(j1, fIdx) - fta1.getFieldStartOffset(j1, fIdx);
                    int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength()
                            + fta2.getFieldStartOffset(j2, fIdx);
                    int l2 = fta2.getFieldEndOffset(j2, fIdx) - fta2.getFieldStartOffset(j2, fIdx);
                    int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }
        };
    }
}
