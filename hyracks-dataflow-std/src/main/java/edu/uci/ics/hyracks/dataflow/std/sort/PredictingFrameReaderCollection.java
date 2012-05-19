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
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class PredictingFrameReaderCollection {
    private final IHyracksTaskContext ctx;
    private final RecordDescriptor recordDesc;
    private final IFrameReader[] runCursors;
    private final IFrameReader[] fileReaders;
    private Comparator<ReferenceEntry> comparator;

    private final FrameTupleAccessor[] tupleAccessors;

    // Holds the reference to the last-tuple of each frame being sorted.
    private ReferencedPriorityQueue backQueue;

    /* The two blocking queues which hold the buffer and the runIndex of the corresponding buffer frame which are
     * used for prediction. The empty queue contains all the buffers that are available for prediction and the
     * full queue contains all the populated buffers.
     */
    private ArrayBlockingQueue<PredictionBuffer> emptyPredictionQueue;
    private final ForecastQueues forecastQueues;

    private final Thread predictorThread;

    // Constructor for PredictingFrameReaderCollection.
    public PredictingFrameReaderCollection(IHyracksTaskContext ctx, RecordDescriptor recordDesc, IFrameReader[] runs,
            int predictionFramesLimit) {
        this.ctx = ctx;
        this.recordDesc = recordDesc;
        this.fileReaders = new IFrameReader[runs.length];

        this.runCursors = new IFrameReader[runs.length];
        for (int i = 0; i < runs.length; i++) {
            fileReaders[i] = runs[i];
            runCursors[i] = new PredictingFrameReader(fileReaders[i], i, this);
        }

        // As a guard, initialize the comparator is set to null, so accesses won't raise a compilation error.
        this.comparator = null;

        this.emptyPredictionQueue = new ArrayBlockingQueue<PredictionBuffer>(predictionFramesLimit);

        for (int i = 0; i < predictionFramesLimit; i++) {
            this.emptyPredictionQueue.add(new PredictionBuffer(ctx, this.fileReaders));
        }

        forecastQueues = new ForecastQueues(runs.length);

        tupleAccessors = new FrameTupleAccessor[runs.length];

        locks = new Object[runs.length];

        for (int i = 0; i < runs.length; i++) {
            tupleAccessors[i] = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            locks[i] = new Object();
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
                success = forecastQueues.deque(buffer, runIndex);
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
        int runIndex;
        backQueue = new ReferencedPriorityQueue(ctx.getFrameSize(), recordDesc, tupleAccessors.length, comparator);
        for (int i = 0; i < tupleAccessors.length; i++) {
            runIndex = backQueue.peek().getRunid();
            backQueue.popAndReplace(tupleAccessors[runIndex], tupleAccessors[runIndex].getTupleCount() - 1);
        }
    }

    private class ForecastQueues {
        private final PredictionBuffer[] heads;
        private final PredictionBuffer[] tails;

        public ForecastQueues(int numQueues) {
            heads = new PredictionBuffer[numQueues];
            tails = new PredictionBuffer[numQueues];

            for (int i = 0; i < numQueues; i++) {
                heads[i] = null;
                tails[i] = null;
            }
        }

        public boolean enqueue(ReferenceEntry entry, PredictionBuffer predBuffer) throws HyracksDataException,
                InterruptedException {
            PredictionBuffer prevTail;
            /* We always keep pre-fetching the next frame for the predicted run as long as we have
             * an empty buffer to predict, i.e. the emptyPredictionQueue is NOT empty. Since
             * emptyPredictionQueue is implemented as an ArrayBlockingQueue object this
             * automatically blocks this thread and handles the signalling when the
             * emptyPredictionQueue is empty so as not to proceed.
             */
            int runIndex = entry.getRunid();
            FrameTupleAccessor fta = entry.getAccessor();

            boolean hasBuffer = predBuffer.nextRunFrame(runIndex);
            if (!hasBuffer) {
                emptyPredictionQueue.put(predBuffer);
                return false;
            }

            prevTail = tails[runIndex];
            tails[runIndex] = predBuffer;
            fta.reset(predBuffer.getBuffer());
            entry.setTupleIndex(fta.getTupleCount() - 1);
            if (heads[runIndex] == null) {
                heads[runIndex] = predBuffer;
                locks[0].notifyAll();
            } else {
                prevTail.setNext(predBuffer);
            }
            return hasBuffer;
        }

        public boolean deque(ByteBuffer buffer, int runIndex) throws InterruptedException {
            FrameTupleAccessor fta = null;
            PredictionBuffer head = null;

            synchronized (locks[0]) {
                fta = tupleAccessors[runIndex];

                if ((heads[runIndex] == null) && (!backQueue.runIndexExists(runIndex))) {
                    return false;
                }

                while (heads[runIndex] == null) {
                    locks[0].wait();
                    if (!backQueue.runIndexExists(runIndex)) {
                        return false;
                    }
                }

                head = heads[runIndex];
                PredictionBuffer headNext = head.getNext();
                head.setNext(null);
                head.transfer(buffer);

                if (headNext == null) {
                    fta.reset(buffer);
                }
                heads[runIndex] = headNext;

                emptyPredictionQueue.put(head);
            }

            return true;
        }
    }

    private class PredictionBuffer {
        private final IFrameReader[] fileReaders;
        private final ByteBuffer buffer;
        private PredictionBuffer next;
        private boolean hasBuffer;

        public PredictionBuffer(IHyracksTaskContext ctx, IFrameReader[] fileReaders) {
            this.buffer = ctx.allocateFrame();
            this.fileReaders = fileReaders;
            this.next = null;
            this.hasBuffer = false;
        }

        public boolean nextRunFrame(int runIndex) throws HyracksDataException {
            buffer.clear();
            hasBuffer = fileReaders[runIndex].nextFrame(buffer);
            return hasBuffer;
        }

        public void transfer(ByteBuffer buffer) {
            FrameUtils.copy(this.buffer, buffer);
            hasBuffer = false;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public PredictionBuffer getNext() {
            return next;
        }

        public void setNext(PredictionBuffer next) {
            this.next = next;
        }

        public boolean hasValidBuffer() {
            return hasBuffer;
        }
    }

    private class PredictorThread implements Runnable {
        public void run() {
            ReferenceEntry entry;
            PredictionBuffer predBuffer;

            while (!backQueue.areRunsExhausted()) {
                entry = backQueue.peek();

                try {
                    predBuffer = emptyPredictionQueue.take();
                    synchronized (locks[0]) {
                        if (forecastQueues.enqueue(entry, predBuffer)) {
                            backQueue.popAndReplace(entry.getAccessor(), entry.getAccessor().getTupleCount() - 1);
                        } else {
                            backQueue.pop();
                            locks[0].notifyAll();
                        }
                    }
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
