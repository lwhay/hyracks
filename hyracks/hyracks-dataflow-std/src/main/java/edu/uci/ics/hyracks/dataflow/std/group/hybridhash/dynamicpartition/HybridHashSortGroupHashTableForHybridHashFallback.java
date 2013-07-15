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

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.util.FrameTupleAccessorForGroupHashtable;

public class HybridHashSortGroupHashTableForHybridHashFallback extends HybridHashSortGroupHashTable {

    private final IAggregatorDescriptor internalAggregator;

    private final ITuplePartitionComputer internalTpc;

    public HybridHashSortGroupHashTableForHybridHashFallback(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int[] keys, IBinaryComparator[] comparators, ITuplePartitionComputer rawTpc,
            ITuplePartitionComputer internalTpc, INormalizedKeyComputer firstNormalizerComputer,
            IAggregatorDescriptor rawAggregator, IAggregatorDescriptor internalAggregator, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc) throws HyracksDataException {
        super(ctx, frameLimits, tableSize, keys, comparators, rawTpc, firstNormalizerComputer, rawAggregator,
                inRecDesc, outRecDesc);
        this.internalAggregator = internalAggregator;
        this.internalTpc = internalTpc;
    }

    public void insert(FrameTupleAccessor accessor, int tupleIndex, boolean isRawInput) throws HyracksDataException {

        ITuplePartitionComputer workTpc = isRawInput ? tpc : internalTpc;
        IAggregatorDescriptor workAggergator = isRawInput ? aggregator : internalAggregator;
        int[] ks = (isRawInput) ? keys : internalKeys;

        int entry = workTpc.partition(accessor, tupleIndex, tableSize);

        hashedRawRec++;

        if (findMatch(entry, accessor, tupleIndex, isRawInput)) {
            // find match; do aggregation
            hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
            int htRecordStartOffset = hashtableRecordAccessor.getTupleStartOffset(matchPointer.tupleIndex);
            workAggergator.aggregate(accessor, tupleIndex, hashtableRecordAccessor.getBuffer().array(),
                    htRecordStartOffset, hashtableRecordAccessor.getTupleEndOffset(matchPointer.tupleIndex)
                            - htRecordStartOffset, aggState);
        } else {

            internalTupleBuilder.reset();
            for (int k = 0; k < ks.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, ks[k]);
            }
            workAggergator.init(internalTupleBuilder, accessor, tupleIndex, aggState);
            int insertFrameIndex = -1, insertTupleIndex = -1;
            boolean inserted = false;

            if (currentLargestFrameIndex < 0) {
                currentLargestFrameIndex = 0;
            }

            if (contents[currentLargestFrameIndex] == null) {
                contents[currentLargestFrameIndex] = ctx.allocateFrame();
            }

            internalAppender.reset(contents[currentLargestFrameIndex], false);
            if (internalAppender.append(internalTupleBuilder.getFieldEndOffsets(), internalTupleBuilder.getByteArray(),
                    0, internalTupleBuilder.getSize())) {
                inserted = true;
                insertFrameIndex = currentLargestFrameIndex;
                insertTupleIndex = internalAppender.getTupleCount() - 1;
            }

            if (!inserted && currentLargestFrameIndex < contents.length - 1) {
                currentLargestFrameIndex++;
                if (contents[currentLargestFrameIndex] == null) {
                    contents[currentLargestFrameIndex] = ctx.allocateFrame();
                }
                internalAppender.reset(contents[currentLargestFrameIndex], true);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to insert an aggregation value.");
                } else {
                    insertFrameIndex = currentLargestFrameIndex;
                    insertTupleIndex = internalAppender.getTupleCount() - 1;
                    inserted = true;
                }
            }

            // memory is full
            if (!inserted) {
                // flush hash table and try to insert again
                flush();

                // update the match point to the header reference
                matchPointer.frameIndex = -1;
                matchPointer.tupleIndex = -1;
                // re-insert
                currentLargestFrameIndex++;
                if (contents[currentLargestFrameIndex] == null) {
                    contents[currentLargestFrameIndex] = ctx.allocateFrame();
                }
                internalAppender.reset(contents[currentLargestFrameIndex], true);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to insert an aggregation value.");
                } else {
                    insertFrameIndex = currentLargestFrameIndex;
                    insertTupleIndex = internalAppender.getTupleCount() - 1;
                }
            }

            // no match; new insertion
            if (matchPointer.frameIndex < 0) {
                // first record for this entry; update the header references
                int headerFrameIndex = getHeaderFrameIndex(entry);
                int headerFrameOffset = getHeaderTupleIndex(entry);
                if (headers[headerFrameIndex] == null) {
                    headers[headerFrameIndex] = ctx.allocateFrame();
                    resetHeader(headerFrameIndex);
                }
                headers[headerFrameIndex].putInt(headerFrameOffset, insertFrameIndex);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, insertTupleIndex);
                usedEntries++;

            } else {
                // update the previous reference
                hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                contents[matchPointer.frameIndex].putInt(refOffset, insertFrameIndex);
                contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE, insertTupleIndex);
            }
            hashedKeys++;
            totalTupleCount++;
        }
    }

    protected boolean findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex, boolean isRawInput) {

        // reset the match pointer
        matchPointer.frameIndex = -1;
        matchPointer.tupleIndex = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            matchPointer.frameIndex = entryFrameIndex;
            matchPointer.tupleIndex = entryTupleIndex;
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            if (compare(accessor, tupleIndex, hashtableRecordAccessor, entryTupleIndex, isRawInput) == 0) {
                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
        }
        return false;
    }

    private int compare(FrameTupleAccessor accessor, int tupleIndex, FrameTupleAccessorForGroupHashtable hashAccessor,
            int hashTupleIndex, boolean isRawInput) {

        int[] ks = isRawInput ? keys : internalKeys;

        int tStart0 = accessor.getTupleStartOffset(tupleIndex);
        int fStartOffset0 = accessor.getFieldSlotsLength() + tStart0;

        int tStart1 = hashAccessor.getTupleStartOffset(hashTupleIndex);
        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < ks.length; ++i) {
            int fStart0 = accessor.getFieldStartOffset(tupleIndex, ks[i]);
            int fEnd0 = accessor.getFieldEndOffset(tupleIndex, ks[i]);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = hashAccessor.getFieldStartOffset(hashTupleIndex, internalKeys[i]);
            int fEnd1 = hashAccessor.getFieldEndOffset(hashTupleIndex, internalKeys[i]);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0, hashAccessor
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

}
