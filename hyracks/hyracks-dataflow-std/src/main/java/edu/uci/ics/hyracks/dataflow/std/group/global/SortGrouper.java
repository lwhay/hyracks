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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.GroupRunMergingFrameReader;
import edu.uci.ics.hyracks.dataflow.std.sort.FrameSorter;

public class SortGrouper implements IFrameWriter {

    private final IHyracksTaskContext ctx;
    private final FrameSorter frameSorter;
    private final IAggregatorDescriptor aggregator;
    private final int framesLimit;
    private final RecordDescriptor inRecordDesc, outRecordDesc;
    private final IBinaryComparatorFactory[] comparatorFactoreis;

    private AggregateState aggregateState;

    private RunningAggregateFrameWriter raWriter;

    public SortGrouper(IHyracksTaskContext ctx, int[] keyFields, int framesLimit,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptor aggregator, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.framesLimit = framesLimit;
        this.inRecordDesc = inRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.comparatorFactoreis = comparatorFactories;
        this.frameSorter = new FrameSorter(ctx, keyFields, firstKeyNormalizerFactory, comparatorFactories, inRecordDesc);
        this.aggregator = aggregator;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        aggregateState = aggregator.createAggregateStates();
        frameSorter.reset();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (frameSorter.getFrameCount() >= framesLimit - 1) {
            flushFrames();
        }
        frameSorter.insertFrame(buffer);
    }

    /**
     * Aggregated and flush the sorted frames.
     * 
     * @throws HyracksDataException
     */
    private void flushFrames() throws HyracksDataException {
        if (raWriter == null) {
            raWriter = new RunningAggregateFrameWriter();
            raWriter.open();
        }
        // sort frames
        frameSorter.sortFrames();
        frameSorter.flushFrames(raWriter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#close()
     */
    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    private class RunningAggregateFrameWriter implements IFrameWriter {

        /**
         * The memory cache for running aggregation.
         */
        ByteBuffer groupingCache;
        FrameTupleAccessor inFrameAccessor, groupingCacheAccessor;
        FrameTupleAppender appender;
        ArrayTupleBuilder tupleBuilder;
        ByteBuffer outputFrame;
        IBinaryComparator[] comparators;

        RunningAggregateFrameWriter() {
            inFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDesc);
            appender = new FrameTupleAppender(ctx.getFrameSize());
            tupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFieldCount());
            comparators = new IBinaryComparator[comparatorFactoreis.length];
            for (int i = 0; i < comparators.length; i++) {
                comparators[i] = comparatorFactoreis[i].createBinaryComparator();
            }
        }

        @Override
        public void open() throws HyracksDataException {
            outputFrame = ctx.allocateFrame();
            tupleBuilder.reset();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            inFrameAccessor.reset(buffer);
            int tupleCount = inFrameAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                if (groupingCache != null && groupingCacheAccessor.getTupleCount() > 0) {
                    // check whether there is a match
                    groupingCacheAccessor.reset(groupingCache);

                }
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            // TODO Auto-generated method stub

        }

        @Override
        public void close() throws HyracksDataException {
            // TODO Auto-generated method stub

        }

        private boolean isSameGroup(FrameTupleAccessor accessor, int tupleIndex) {
            for (int i = 0; i < comparators.length; i++) {

            }
            return true;
        }
    }

}
