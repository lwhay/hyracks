/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class PredictingFrameReaderCollection {
    private final IHyracksTaskContext ctx;
    private final IFrameReader[] runCursors;
    private final List<IFrameReader> runs;
    private final IFrameReader[] fileReaders;
    private int[] tupleIndexes;
    private ReferencedPriorityQueue topTuples;
    private FrameTupleAccessor[] tupleAccessors;
    private Comparator<ReferenceEntry> comparator;

    private ByteBuffer predictedFrame;
    private int predictedFrameRunIndex;
    private final Object predictorLock = new Object();

    // Holds the reference to the last-tuple of each frame being sorted.
    private ReferenceEntry[] backQueue;

    /* Holds a mapping from the runIndex to the position where the
     * ReferenceEntry object corresponding to this runIndex exists in
     * the backQueue. This is necessary for easier/faster lookups.
     */
    private int[] runIndexBackQueueIndexMap;

    // Constructor for PredictingFrameReaderCollection.
    public PredictingFrameReaderCollection(IHyracksTaskContext ctx,
            IFrameReader[] runCursors, List<IFrameReader> runs) {
        this.ctx = ctx;
        this.runCursors = runCursors;
        this.runs = runs;
        this.fileReaders = new IFrameReader[runCursors.length];

        /* As a guard, initialize these objects to null, so accesses won't
         * raise a compilation error.
         */
        this.tupleIndexes = null;
        this.topTuples = null;
        this.tupleAccessors = null;

        predictedFrame = this.ctx.allocateFrame();
        predictedFrameRunIndex = -1;

        int backQueueSize;
        // Initialize backQueue, its size and its entries.
        backQueueSize = (runCursors.length + 1) & 0xfffffffe;
        backQueue = new ReferenceEntry[backQueueSize];

        runIndexBackQueueIndexMap = new int[runCursors.length];

        // Start the predictor thread.
        Thread predictorThread = new Thread(new PredictorThread());
        predictorThread.start();
    }

    public IFrameReader getPredictingFrameReader(int index) {
        fileReaders[index] = runs.get(index);
        runCursors[index] = new PredictingFrameReader(
                fileReaders[index], index, this);

        return runCursors[index];
    }

    public void setMergerParams(
            int[] tupleIndexes, ReferencedPriorityQueue topTuples,
            FrameTupleAccessor[] tupleAccessors,
            Comparator<ReferenceEntry> comparator) {
        this.tupleIndexes = tupleIndexes;
        this.topTuples = topTuples;
        this.tupleAccessors = tupleAccessors;
        this.comparator = comparator;

        buildBackQueue();
    }

    public void getFrame(ByteBuffer buffer, int runIndex)
            throws HyracksDataException {
        if (runIndex == predictedFrameRunIndex) {
            /* Ugly solution. What we should instead do is, get hold of the
             * inFrames array containing ByteBuffers corresponding to a frame
             * of each run and then just swap out the existing the buffer
             * object of the run specified by the given index to the predicted
             * buffer object.
             */
            synchronized(predictorLock) {
                buffer.put(predictedFrame);
                buffer.flip();
                predictedFrame.clear();
                predictedFrameRunIndex = -1;
            }
            rebuildBackQueue(0);
        } else {
            /* Prediction has failed. We can enter this block in two cases.
             * First being the case when we are fetching the first set of
             * frames for the run and the second case being when we have
             * actually started predicting, but the prediction has actually
             * failed. In either case we should just read from the file to
             * the buffer.
             */
            fileReaders[runIndex].nextFrame(buffer);
            if (tupleAccessors != null) {
                rebuildBackQueue(runIndexBackQueueIndexMap[runIndex]);
            }
        }
    }

    private void buildBackQueue() {
        /* We first populate the heap and then build the heap.
         * NOTE: Building the heap is an O( n log(n) ) operation, but
         * populating the heap is an O( n ) operation. So heap building
         * dominates the populating stage, so we can afford to run the
         * populating stage as a pre-processing step.
         */
        for (int i = 0; i < backQueue.length; i++) {
            backQueue[i] = new ReferenceEntry(i, tupleAccessors[i],
                    tupleAccessors[i].getTupleCount() - 1);
        }

        /* Start from the parent of the last leaf node up until the root
         * to heapify.
         */
        for (int i = ((backQueue.length - 1) >> 1); i >= 0; i--) {
            siftDown(i);
        }
    }

    private void swapBackQueueEntries(
            int queueIndex1, int queueIndex2) {
        // Swap the backQueue entries.
        ReferenceEntry tmp = backQueue[queueIndex1];
        backQueue[queueIndex1] = backQueue[queueIndex2];
        backQueue[queueIndex2] = tmp;

        // Update their positions in the runIndexBackQueueIndexMap
        runIndexBackQueueIndexMap[backQueue[queueIndex1].getRunid()] =
                queueIndex1;
        runIndexBackQueueIndexMap[backQueue[queueIndex2].getRunid()] =
                queueIndex2;
    }

    private void siftUp(int queueIndex) {
        if (queueIndex > 0) {
            /* Indexes start from 0, so an adjustment into the queueIndex is
             * needed to find the parent.
             */
            int parentIndex = (queueIndex - 1) >> 1;
            if (comparator.compare(
                    backQueue[queueIndex], backQueue[parentIndex]) <= 0) {
                swapBackQueueEntries(parentIndex, queueIndex);
                siftUp(parentIndex);
            }
        }
    }

    private void siftDown(int parentIndex) {
        int childIndex;

        /* Indexes start from 0, so an adjustment into the leftChildIndex and
         * rightChildIndex is needed, so 1 and 2 are added respectively as
         * an adjustment.
         */
        int leftChildIndex = (parentIndex << 1) + 1;
        int rightChildIndex = (parentIndex << 1) + 2;
        if ((rightChildIndex < backQueue.length) && (comparator.compare(
                backQueue[rightChildIndex], backQueue[leftChildIndex]) <= 0)) {
            childIndex = rightChildIndex;
        } else {
            childIndex = leftChildIndex;
        }

        if ((childIndex < backQueue.length) && (comparator.compare(
                backQueue[childIndex], backQueue[parentIndex]) <= 0)) {
            swapBackQueueEntries(parentIndex, childIndex);
            siftDown(childIndex);
        }
    }

    /* Reconstructs the heap by assuming that the last tuple of the leading
     * entry of the backQueue has changed because a new frame as been read
     * for that run. This works under the assumption that in the getFrame()
     * method we always enter the if-block and never the else-block so that
     * the leading entry of the backQueue corresponds to exactly the same
     * run for which the new frame was predicted.
     */
    private void rebuildBackQueue(int queueIndex) {
        siftUp(queueIndex);
        siftDown(queueIndex);
    }

    private class PredictorThread implements Runnable {
        public void run() {
            try {
                while (!topTuples.areRunsExhausted()) {
                    /* If the leading entry of the back-queue is close to
                     * getting exhausted, this proximity hard coded to half
                     * the size of the frame for now, we predict that the
                     * frame for the corresponding run will get exhausted
                     * first and hence pre-fetch that frame.
                     */
                    int lastTupleIndex = backQueue[0].getTupleIndex();
                    int runIndex = backQueue[0].getRunid();

                    if ((lastTupleIndex - tupleIndexes[runIndex]) <=
                            (lastTupleIndex / 2)) {
                        synchronized(predictorLock) {
                            try {
                                fileReaders[runIndex].nextFrame(predictedFrame);
                                predictedFrameRunIndex = runIndex;
                            } catch (HyracksDataException e) {
                                /* Reading from the file failed, but we are
                                 * inside a thread and cannot do much, so just
                                 * pass. This will be caught later when the same
                                 * frame is attempted to be read again.
                                 */
                            }
                        }
                    }

                    /* FIXME: Hard-coded to 0.5 seconds. The rationale behind
                     * this is that the disk seek times today are about this
                     * long, so we may not want to predict for this long.
                     * But this has the problem of prediction failing
                     * most of the times on a machine with very fast CPU. On
                     * the other hand this thread may interrupt other things
                     * too often on machines with very slow CPU.
                     * The most ideal solution would be to make any updates
                     * to runIndexes notify any other threads that is waiting
                     * for it and make this thread wait for it and invoke
                     * when notified. Even better would be to be invoked
                     * when the runIndex value for any run is half way through
                     * the frame for any run.
                     */
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                // Since thread was interrupted, we can just bail out.
                return;
            }
        }
    }
}
