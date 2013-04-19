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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition;

import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class OriginalHybridHashGroupHashTable extends AbstractHybridHashDynamicPartitionGroupHashTable {

    public OriginalHybridHashGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int numOfPartitions, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily rawTpcf, ITuplePartitionComputerFamily intermediateTpcf,
            IAggregatorDescriptor rawAggregator, IAggregatorDescriptor internalAggregator, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter) throws HyracksDataException {
        super(ctx, frameLimits, tableSize, numOfPartitions, procLevel, keys, comparators, rawTpcf, intermediateTpcf,
                rawAggregator, internalAggregator, inRecDesc, outRecDesc, outputWriter);

        this.rawHashTpc = rawTpcf.createPartitioner(procLevel + 1);
        this.intermediateHashTpc = intermediateTpcf.createPartitioner(procLevel + 1);

        // pre-allocate one frame for each partition
        for (int i = 0; i < this.numOfPartitions; i++) {
            partitionTopPages[i] = frameManager.allocateFrame();
            if (partitionTopPages[i] < 0) {
                throw new HyracksDataException("Failed to allocate an output buffer for partition " + i);
            }
            partitionSizeInFrames[i] = 1;
        }
        // by default spilling all non-resident partitions
        for (int i = 1; i < this.numOfPartitions; i++) {
            setPartitionSpilled(i);
        }
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.AbstractHybridHashDynamicPartitionGroupHashTable#insert(edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor, int, boolean)
     */
    @Override
    protected void insert(FrameTupleAccessor accessor, int tupleIndex, boolean isRawInput) throws HyracksDataException {
        if (inputRecordState == 1 && isRawInput) {
            // start inserting raw records; all aggregated records should be spilled
            for (int i = 0; i < numOfPartitions; i++) {
                if (isPartitionSpilled(i)) {
                    flushSpilledPartition(i);
                }
            }
            inputRecordState = 2;
        }

        inputRecordState = isRawInput ? 2 : 1;

        IAggregatorDescriptor aggregator = (isRawInput) ? rawAggregator : intermediateAggregator;

        int[] ks = (isRawInput) ? keys : internalKeys;

        int h = isRawInput ? rawHashTpc.partition(accessor, tupleIndex, Integer.MAX_VALUE) : intermediateHashTpc
                .partition(accessor, tupleIndex, Integer.MAX_VALUE);

        int entry = h % tableSize;

        int pid = partition(h);

        if (isPartitionSpilled(pid)) {
            // for spilled partition: direct insertion

            insertSpilledPartition(accessor, tupleIndex, pid);

        } else {
            hashedRawRecords++;
            boolean foundMatch = findMatch(entry, accessor, tupleIndex);
            if (foundMatch) {
                // find match; do aggregation
                hashtableRecordAccessor.reset(frameManager.getFrame(matchPointer[0]));
                aggregator.aggregate(accessor, tupleIndex, frameManager.getFrame(matchPointer[0]).array(),
                        matchPointer[1],
                        tupleAccessHelper.getTupleLength(frameManager.getFrame(matchPointer[0]), matchPointer[1]),
                        partitionAggregateStates[pid]);
            } else {

                if (partitionAggregateStates[pid] == null) {
                    partitionAggregateStates[pid] = aggregator.createAggregateStates();
                }

                internalTupleBuilder.reset();
                for (int k = 0; k < ks.length; k++) {
                    internalTupleBuilder.addField(accessor, tupleIndex, ks[k]);
                }
                aggregator.init(internalTupleBuilder, accessor, tupleIndex, partitionAggregateStates[pid]);

                internalAppender.reset(frameManager.getFrame(partitionTopPages[pid]), false);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {

                    // try to allocate a new frame
                    while (!isPartitionSpilled(pid)) {
                        int newFrame = frameManager.allocateFrame();
                        if (newFrame != END_REF) {
                            frameManager.setNextFrame(newFrame, partitionTopPages[pid]);
                            partitionTopPages[pid] = newFrame;
                            partitionSizeInFrames[pid]++;

                            internalAppender.reset(frameManager.getFrame(partitionTopPages[pid]), true);
                            if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                    internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                                throw new HyracksDataException(
                                        "Failed to insert an aggregation value to the hash table.");
                            }

                            break;
                        } else {
                            // choose a partition to spill to make space
                            chooseResidentPartitionToFlush();
                            if (isPartitionSpilled(pid)) {
                                // in case that the current partition is spilled
                                insertSpilledPartition(accessor, tupleIndex, pid);
                                break;
                            }
                        }
                    }
                }

                // update hash table reference, only if the insertion to a resident partition is successful
                if (!isPartitionSpilled(pid)) {

                    hashedKeys++;

                    // update hash reference
                    if (matchPointer[0] < 0) {
                        // first record for this entry; update the header references
                        int headerFrameIndex = getHeaderFrameIndex(entry);
                        int headerFrameOffset = getHeaderTupleIndex(entry);

                        frameManager.getFrame(headers[headerFrameIndex]).putInt(headerFrameOffset,
                                partitionTopPages[pid]);
                        frameManager.getFrame(headers[headerFrameIndex]).putInt(headerFrameOffset + INT_SIZE,
                                getTupleStartOffset(partitionTopPages[pid], internalAppender.getTupleCount() - 1));

                    } else {
                        // update the previous reference
                        int refOffset = tupleAccessHelper.getTupleEndOffset(frameManager.getFrame(matchPointer[0]),
                                matchPointer[1]);

                        frameManager.getFrame(matchPointer[0]).putInt(refOffset, partitionTopPages[pid]);
                        frameManager.getFrame(matchPointer[0]).putInt(refOffset + INT_SIZE,
                                getTupleStartOffset(partitionTopPages[pid], internalAppender.getTupleCount() - 1));
                    }
                }
            }
        }
        partitionRawRecordCounts[pid]++;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.AbstractHybridHashDynamicPartitionGroupHashTable#chooseResidentPartitionToFlush()
     */
    @Override
    protected void chooseResidentPartitionToFlush() throws HyracksDataException {
        int partitionToPick = -1;
        // choose one resident partition to spill. Note that after this loop 
        // it always gets a resident partition in partitionToPick, if there is any.
        for (int i = 0; i < numOfPartitions; i++) {

            if (isPartitionSpilled(i)) {
                continue;
            }

            if (partitionToPick < 0) {
                partitionToPick = i;
                continue;
            }
            if (partitionSizeInFrames[i] > partitionSizeInFrames[partitionToPick]) {
                partitionToPick = i;
            }
        }

        if (partitionToPick < 0 || partitionToPick >= numOfPartitions) {
            // no resident partition to spill
            return;
        }

        // flush the partition picked
        spillPartition(partitionToPick, false);

        // mark the picked partition as spilled
        setPartitionSpilled(partitionToPick);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.AbstractHybridHashDynamicPartitionGroupHashTable#finishup()
     */
    @Override
    public void finishup() throws HyracksDataException {
        // flush all spilled partitions
        partitionRawRecords = new LinkedList<Integer>();
        runReaders = new LinkedList<IFrameReader>();
        runsAggregatedPages = new LinkedList<Integer>();
        partitionSizeInPages = new LinkedList<Integer>();
        for (int i = 0; i < numOfPartitions; i++) {
            if (!isPartitionSpilled(i)) {
                continue;
            }
            flushSpilledPartition(i);
            if (runWriters[i] != null) {
                runReaders.add(runWriters[i].createReader());
                runsAggregatedPages.add(partitionAggregatedPages[i]);
                runWriters[i].close();
                runWriters[i] = null;
                partitionRawRecords.add(partitionRawRecordCounts[i]);
                partitionSizeInPages.add(partitionSizeInFrames[i]);
            }
        }

        // flush resident partitions
        if (outputBuffer >= 0)
            outputAppender.reset(frameManager.getFrame(outputBuffer), true);
        for (int i = 0; i < numOfPartitions; i++) {
            if (isPartitionSpilled(i)) {
                continue;
            }
            if (!firstOutputMarker) {
                ctx.getCounterContext().getCounter("must.time." + procLevel + ".firstOutput", true)
                        .set(System.nanoTime());
                firstOutputMarker = true;
            }
            spillPartition(i, true);
        }

        ctx.getCounterContext().getCounter("must.hash.succ.comps", true).update(hashSuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.unsucc.comps", true).update(hashUnsuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.slot.init", true).update(hashInitCount);

        hashSuccComparisonCount = 0;
        hashUnsuccComparisonCount = 0;
        hashInitCount = 0;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.AbstractHybridHashDynamicPartitionGroupHashTable#open()
     */
    @Override
    public void open() throws HyracksDataException {
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.AbstractHybridHashDynamicPartitionGroupHashTable#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.AbstractHybridHashDynamicPartitionGroupHashTable#isAllPartitionsSpilled()
     */
    @Override
    protected boolean isAllPartitionsSpilled() {
        for (int i = 0; i < numOfPartitions; i++) {
            if (!isPartitionSpilled(i)) {
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartition.AbstractHybridHashDynamicPartitionGroupHashTable#getHeaderSize(int, int)
     */
    @Override
    protected int getHeaderSize(int tableSize, int frameSize) {
        int residual = ((tableSize % frameSize) * INT_SIZE * 2) % frameSize == 0 ? 0 : 1;
        return (tableSize / frameSize * 2 * INT_SIZE) + ((tableSize % frameSize) * 2 * INT_SIZE / frameSize) + residual;
    }

}
