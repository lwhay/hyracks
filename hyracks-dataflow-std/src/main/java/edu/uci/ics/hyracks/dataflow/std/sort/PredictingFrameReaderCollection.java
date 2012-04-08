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
import java.util.concurrent.ArrayBlockingQueue;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.SelectionTree;
import edu.uci.ics.hyracks.dataflow.std.util.SelectionTree.Entry;

public class PredictingFrameReaderCollection {
    private final IFrameReader[] runCursors;
    private final IFrameReader[] fileReaders;
    private Comparator<ReferenceEntry> comparator;

    private final FrameTupleAccessor[] tupleAccessors;

    // Each entry corresponding to the last tuple corresponding to the frame of the current run being sorted.
    private final BackQueueEntry[] backQueueEntries;

    // Holds the reference to the last-tuple of each frame being sorted.
    private SelectionTree backQueue;

    /* The two blocking queues which hold the buffer and the runIndex of the corresponding buffer frame which are
     * used for prediction. The empty queue contains all the buffers that are available for prediction and the
     * full queue contains all the populated buffers.
     */
    private ArrayBlockingQueue<PredictionBuffer> emptyPredictionQueue;
    private ArrayBlockingQueue<PredictionBuffer> fullPredictionQueue;

    private final Thread predictorThread;

    // Constructor for PredictingFrameReaderCollection.
    public PredictingFrameReaderCollection(IHyracksTaskContext ctx, RecordDescriptor recordDesc, IFrameReader[] runs,
            int predictionFramesLimit) {
        this.fileReaders = new IFrameReader[runs.length];

        this.runCursors = new IFrameReader[runs.length];
        for (int i = 0; i < runs.length; i++) {
            fileReaders[i] = runs[i];
            runCursors[i] = new PredictingFrameReader(fileReaders[i], i, this);
        }

        // As a guard, initialize the comparator is set to null, so accesses won't raise a compilation error.
        this.comparator = null;

        this.emptyPredictionQueue = new ArrayBlockingQueue<PredictionBuffer>(predictionFramesLimit);
        this.fullPredictionQueue = new ArrayBlockingQueue<PredictionBuffer>(predictionFramesLimit);

        for (int i = 0; i < predictionFramesLimit; i++) {
            this.emptyPredictionQueue.add(new PredictionBuffer(ctx, this.fileReaders));
        }

        tupleAccessors = new FrameTupleAccessor[runs.length];
        backQueueEntries = new BackQueueEntry[runs.length];

        for (int i = 0; i < runs.length; i++) {
            tupleAccessors[i] = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            backQueueEntries[i] = new BackQueueEntry(i, tupleAccessors[i], -1);
        }

        // Start the predictor thread.
        predictorThread = new Thread(new PredictorThread());
    }

    public IFrameReader[] getRunCursors() {
        return runCursors;
    }

    public void setComparator(Comparator<ReferenceEntry> comparator) {
        this.comparator = comparator;

        buildBackQueue();
        predictorThread.start();
    }

    public boolean getFrame(ByteBuffer buffer, int runIndex) throws HyracksDataException {
        boolean success;
        if (comparator == null) {
            // For the initial fetch we will have to read from the file and then set the tuple accessors etc.
            success = fileReaders[runIndex].nextFrame(buffer);
            tupleAccessors[runIndex].reset(buffer);
            backQueueEntries[runIndex].setTupleIndex(tupleAccessors[runIndex].getTupleCount() - 1);
        } else {
            try {
                PredictionBuffer predBuf = fullPredictionQueue.take();
                success = predBuf.transfer(buffer, runIndex);
                emptyPredictionQueue.put(predBuf);
            } catch (InterruptedException e) {
                // When the main thread is interrupted, we can't do much, just return.
                return false;
            }
        }
        return success;
    }

    private void buildBackQueue() {
        /* We first populate the heap and then build the heap.
         * NOTE: Building the heap is an O( n log(n) ) operation, but populating the heap is an O( n )
         * operation. So heap building dominates the populating stage, so we can afford to run the
         * populating stage as a pre-processing step.
         */
        backQueue = new SelectionTree(backQueueEntries);
    }

    private class BackQueueEntry extends ReferenceEntry implements Entry {
        private boolean canAdvance;

        public BackQueueEntry(int runIndex, FrameTupleAccessor fta, int tupleIndex) {
            super(runIndex, fta, tupleIndex);
            canAdvance = true;
        }

        @Override
        public int compareTo(Entry o) {
            return comparator.compare(this, (BackQueueEntry) o);
        }

        @Override
        public boolean advance() {
            return canAdvance;
        }

        public void update(PredictionBuffer predictionBuffer) throws HyracksDataException {
            FrameTupleAccessor fta = getAccessor();
            canAdvance = predictionBuffer.nextRunFrame(getRunid(), getAccessor());
            setTupleIndex(canAdvance ? fta.getTupleCount() - 1 : -1);
        }
    }

    private class PredictionBuffer {
        private final IHyracksTaskContext ctx;
        private final IFrameReader[] fileReaders;
        private ByteBuffer buffer;
        private FrameTupleAccessor fta;
        private int runIndex;
        private boolean hasBuffer;

        public PredictionBuffer(IHyracksTaskContext ctx, IFrameReader[] fileReaders) {
            this.ctx = ctx;
            this.fileReaders = fileReaders;
            this.buffer = this.ctx.allocateFrame();
            this.runIndex = -1;
            this.fta = null;
        }

        public boolean nextRunFrame(int runIndex, FrameTupleAccessor fta) throws HyracksDataException {
            buffer.clear();
            hasBuffer = fileReaders[runIndex].nextFrame(buffer);
            buffer.flip();
            this.runIndex = runIndex;
            this.fta = fta;
            fta.reset(buffer);
            return hasBuffer;
        }

        public boolean transfer(ByteBuffer buffer, int runIndex) throws HyracksDataException {
            if (this.runIndex == runIndex) {
                if (hasBuffer) {
                    buffer.put(this.buffer);
                    buffer.flip();
                    this.fta.reset(buffer);
                    this.fta = null;
                    this.runIndex = -1;
                }
            } else {
                throw new HyracksDataException();
            }
            return hasBuffer;
        }
    }

    private class PredictorThread implements Runnable {
        public void run() {
            BackQueueEntry entry;
            PredictionBuffer predBuf;

            while (!backQueue.isEmpty()) {
                entry = (BackQueueEntry) backQueue.peek();

                try {
                    /* We always keep pre-fetching the next frame for the predicted run as long as we have
                     * an empty buffer to predict, i.e. the emptyPredictionQueue is NOT empty. Since
                     * emptyPredictionQueue is implemented as an ArrayBlockingQueue object this
                     * automatically blocks this thread and handles the signalling when the
                     * emptyPredictionQueue is empty so as not to proceed.
                     *
                     * Once we fetch, we insert this fetched prediction buffer object into
                     * fullPredictionQueue. Again if this is full this thread gets blocked. But by design
                     * it is not possible for fullPredictionQueue to be full without emptyPredictionQueue
                     * to be empty and vice-versa since the total number of prediction buffers available
                     * is equal to the total capacity of either of these queues.
                     *
                     * The sum total of number of elements in the emptyPredictionQueue and
                     * fullPredictionQueue is always equal to the capacity of either of the queues.
                     */
                    predBuf = emptyPredictionQueue.take();
                    entry.update(predBuf);
                    fullPredictionQueue.put(predBuf);
                    backQueue.pop();
                } catch (HyracksDataException e) {
                    /* Reading from the file failed, but we are inside a thread and cannot do much, so
                     * just pass. This will be caught later when the same frame is attempted to be read
                     * again.
                     */
                } catch (InterruptedException e) {
                    // Since the thread was interrupted, we just bail out.
                    return;
                }
            }
        }
    }

}
