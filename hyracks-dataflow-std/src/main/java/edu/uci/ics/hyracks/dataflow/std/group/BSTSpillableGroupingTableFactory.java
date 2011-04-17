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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;

/**
 * @author jarodwen
 */
public class BSTSpillableGroupingTableFactory implements ISpillableTableFactory {

    private static final long serialVersionUID = 1L;

    public BSTSpillableGroupingTableFactory() {

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.ISpillableTableFactory#buildSpillableTable(edu.uci.ics.hyracks.api.context.IHyracksStageletContext, int[], edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory[], edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int)
     */
    @Override
    public ISpillableTable buildSpillableTable(final IHyracksStageletContext ctx, final int[] keyFields,
            IBinaryComparatorFactory[] comparatorFactories, final IAggregatorDescriptorFactory aggregatorFactory,
            final RecordDescriptor inRecordDescriptor, final RecordDescriptor outRecordDescriptor, final int framesLimit)
            throws HyracksDataException {
        final int[] storedKeys = new int[keyFields.length];
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keyFields.length];

        for (int i = 0; i < keyFields.length; i++) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[keyFields[i]];
        }
        final FrameTupleAccessor storedKeysAccessor1 = new FrameTupleAccessor(ctx.getFrameSize(), outRecordDescriptor);

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final FrameTuplePairComparator ftpcPartial = new FrameTuplePairComparator(keyFields, storedKeys, comparators);

        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        final ByteBuffer outFrame = ctx.allocateFrame();

        final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        return new ISpillableTable() {

            /**
             * The count of used frames in the table.
             * Note that this cannot be replaced by {@link #frames} since frames will
             * not be removed after being created.
             */
            private int dataFrameCount;

            private static final int INIT_ACCUMULATORS_SIZE = 8;

            private int[] bstree = new int[INIT_ACCUMULATORS_SIZE * 4];
            private int bstreeSize;

            private int actualFramesLimit = framesLimit;

            private final List<ByteBuffer> frames = new ArrayList<ByteBuffer>();

            private IAggregatorDescriptor aggregator = aggregatorFactory.createAggregator(ctx, inRecordDescriptor,
                    outRecordDescriptor, keyFields);

            @Override
            public void reset() {
                dataFrameCount = -1;
                this.bstreeSize = 0;
            }

            @Override
            public boolean insert(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                if (dataFrameCount < 0)
                    nextAvailableFrame();
                // Start inserting
                int parentPtr = 0, nodePtr = 0;
                boolean isSmaller = true, foundGroup = false;
                int sbIndex = -1, stIndex = -1;
                // Do binary search 
                while (nodePtr < bstreeSize && nodePtr >= 0) {
                    sbIndex = bstree[nodePtr];
                    stIndex = bstree[nodePtr + 1];
                    storedKeysAccessor1.reset(frames.get(sbIndex));
                    int c = ftpcPartial.compare(accessor, tIndex, storedKeysAccessor1, stIndex);
                    parentPtr = nodePtr;
                    if (c == 0) {
                        foundGroup = true;
                        break;
                    } else if (c < 0) {
                        nodePtr = bstree[nodePtr + 2];
                        isSmaller = true;
                    } else {
                        nodePtr = bstree[nodePtr + 3];
                        isSmaller = false;
                    }
                }

                if (!foundGroup) {
                    // If no matching group is found, create a new aggregator
                    // Create a tuple for the new group
                    tupleBuilder.reset();
                    for (int i = 0; i < keyFields.length; i++) {
                        tupleBuilder.addField(accessor, tIndex, keyFields[i]);
                    }
                    aggregator.init(accessor, tIndex, tupleBuilder);
                    if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        if (!nextAvailableFrame()) {
                            return false;
                        } else {
                            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                    tupleBuilder.getSize())) {
                                throw new IllegalStateException("Failed to init an aggregator");
                            }
                        }
                    }
                    sbIndex = dataFrameCount;
                    stIndex = appender.getTupleCount() - 1;
                    // Add entry into the binary search tree
                    bstreeAdd(sbIndex, stIndex, parentPtr, isSmaller);
                } else {
                 // If there is a matching found, do aggregation directly
                    int tupleOffset = storedKeysAccessor1.getTupleStartOffset(stIndex);
                    int fieldCount = storedKeysAccessor1.getFieldCount();
                    int aggFieldOffset = storedKeysAccessor1.getFieldStartOffset(stIndex, keyFields.length);
                    int tupleEnd = storedKeysAccessor1.getTupleEndOffset(stIndex);
                    aggregator.aggregate(accessor, tIndex, storedKeysAccessor1.getBuffer().array(), tupleOffset + 2
                            * fieldCount + aggFieldOffset, tupleEnd - (tupleOffset + 2 * fieldCount + aggFieldOffset));
                }
                return true;
            }

            @Override
            public List<ByteBuffer> getFrames() {
                return frames;
            }

            @Override
            public int getFrameCount() {
                return dataFrameCount;
            }

            @Override
            public void flushFrames(IFrameWriter writer, boolean isPartial) throws HyracksDataException {
                FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                writer.open();
                appender.reset(outFrame, true);
                traversalBSTree(writer, 0, appender, isPartial);
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outFrame, writer);
                }
                aggregator.close();
                System.err.println("**** Flushed gTable.");
            }

            /**
             * Set the working frame to the next available frame in the
             * frame list. There are two cases:<br>
             * 1) If the next frame is not initialized, allocate
             * a new frame.
             * 2) When frames are already created, they are recycled.
             * 
             * @return Whether a new frame is added successfully.
             */
            private boolean nextAvailableFrame() {

                if (actualFramesLimit > framesLimit - bstreeSize * 4 / ctx.getFrameSize())
                    actualFramesLimit = framesLimit - bstreeSize * 4 / ctx.getFrameSize();

                // Return false if the number of frames is equal to the limit.
                if (dataFrameCount + 1 >= actualFramesLimit) {
                    return false;
                }

                if (frames.size() < actualFramesLimit) {
                    // Insert a new frame
                    ByteBuffer frame = ctx.allocateFrame();
                    frame.position(0);
                    frame.limit(frame.capacity());
                    frames.add(frame);
                    appender.reset(frame, true);
                    dataFrameCount++;
                } else {
                    // Reuse an old frame
                    dataFrameCount++;
                    ByteBuffer frame = frames.get(dataFrameCount);
                    frame.position(0);
                    frame.limit(frame.capacity());
                    appender.reset(frame, true);
                }
                return true;
            }

            private void traversalBSTree(IFrameWriter writer, int nodePtr, FrameTupleAppender appender, boolean isPartial)
                    throws HyracksDataException {
                if (nodePtr < 0 || nodePtr >= bstreeSize)
                    return;
                traversalBSTree(writer, bstree[nodePtr + 2], appender, isPartial);

                int bIndex = bstree[nodePtr];
                int tIndex = bstree[nodePtr + 1];
                storedKeysAccessor1.reset(frames.get(bIndex));
                // Reset the tuple for the partial result
                tupleBuilder.reset();
                for (int k = 0; k < keyFields.length; k++) {
                    tupleBuilder.addField(storedKeysAccessor1, tIndex, k);
                }
                if (isPartial)
                    aggregator.outputPartialResult(storedKeysAccessor1, tIndex,
                            tupleBuilder);
                else
                    aggregator.outputResult(storedKeysAccessor1, tIndex,
                            tupleBuilder);
                while (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(),
                        0, tupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outFrame, writer);
                    appender.reset(outFrame, true);
                }
                
                traversalBSTree(writer, bstree[nodePtr + 3], appender, isPartial);
            }

            private void bstreeAdd(int bufferIdx, int tIndex, int parentPtr, boolean isSmaller) {
                while (bstreeSize + 4 > bstree.length) {
                    System.err.println("***** Try to increase the bstree size: " + bstreeSize + ", " + bstree.length);
                    bstree = Arrays.copyOf(bstree, bstreeSize * 2);
                }
                // Update tree link
                if (isSmaller) {
                    bstree[parentPtr + 2] = bstreeSize;
                } else {
                    bstree[parentPtr + 3] = bstreeSize;
                }
                // Add new node
                bstree[bstreeSize++] = bufferIdx;
                bstree[bstreeSize++] = tIndex;
                bstree[bstreeSize++] = -1;
                bstree[bstreeSize++] = -1;
            }

            @Override
            public void sortFrames() {
                // Nothing need to be done, since tuples are naturally sorted
            }
        };
    }

}
