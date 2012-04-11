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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class InMemoryHashJoin {
	
    private final List<ByteBuffer> buffers;
    private final List<BitSet> leftOuterJoinBitSets;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private final FrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private final FrameTuplePairComparator tpComparator;
    private final ByteBuffer outBuffer;
    private final boolean isLeftOuter;
    private final boolean isRightOuter;
    private final ArrayTupleBuilder rightNullTupleBuild;
    private final ArrayTupleBuilder leftNullTupleBuild;
    private final ISerializableTable table;
	private final int tableSize;
    private final TuplePointer storedTuplePointer;
    
    public InMemoryHashJoin(IHyracksTaskContext ctx, int tableSize, FrameTupleAccessor accessor0,
            ITuplePartitionComputer tpc0, FrameTupleAccessor accessor1, ITuplePartitionComputer tpc1,
            FrameTuplePairComparator comparator, boolean isLeftOuter, boolean isRightOuter,
            INullWriter[] rightNullWriters, INullWriter[] leftNullWriters, ISerializableTable table)
            throws HyracksDataException {
    	this.tableSize = tableSize;
       	this.table = table;
       	storedTuplePointer = new TuplePointer();
       	buffers = new ArrayList<ByteBuffer>();
        this.accessorBuild = accessor0;
        this.tpcBuild = tpc0;
        this.accessorProbe = accessor1;
        this.tpcProbe = tpc1;
        appender = new FrameTupleAppender(ctx.getFrameSize());
        tpComparator = comparator;
        outBuffer = ctx.allocateFrame();
        appender.reset(outBuffer, true);
        this.isLeftOuter = isLeftOuter;
        this.isRightOuter = isRightOuter;
        if (isLeftOuter) {
        	leftOuterJoinBitSets = new ArrayList<BitSet>();
            int fieldCountOuter = accessor1.getFieldCount();
            rightNullTupleBuild = new ArrayTupleBuilder(fieldCountOuter);
            DataOutput out = rightNullTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCountOuter; i++) {
                rightNullWriters[i].writeNull(out);
                rightNullTupleBuild.addFieldEndOffset();
            }
        } else {
            rightNullTupleBuild = null;
            leftOuterJoinBitSets = null;
        }
        if (isRightOuter) {
            int fieldCountOuter = accessor0.getFieldCount();
            leftNullTupleBuild = new ArrayTupleBuilder(fieldCountOuter);
            DataOutput out = leftNullTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCountOuter; i++) {
                leftNullWriters[i].writeNull(out);
                leftNullTupleBuild.addFieldEndOffset();
            }
        } else {
            leftNullTupleBuild = null;
        }
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        buffers.add(buffer);
        int bIndex = buffers.size() - 1;
        accessorBuild.reset(buffer);
        int tCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tCount; ++i) {
            int entry = tpcBuild.partition(accessorBuild, i, tableSize);
            storedTuplePointer.frameIndex = bIndex;
            storedTuplePointer.tupleIndex = i;
            table.insert(entry, storedTuplePointer);
        }
        if(isLeftOuter)
        	for (int i=table.getFrameCount()-leftOuterJoinBitSets.size()-1; i>0; i--)
        		leftOuterJoinBitSets.add(new BitSet());
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            int entry = tpcProbe.partition(accessorProbe, i, tableSize);
            boolean matchFound = false;
            int offset = 0;
            do {
                table.getTuplePointer(entry, offset++, storedTuplePointer);
                if (storedTuplePointer.frameIndex < 0)
                    break;
                int bIndex = storedTuplePointer.frameIndex;
                int tIndex = storedTuplePointer.tupleIndex;
                accessorBuild.reset(buffers.get(bIndex));
                int c = tpComparator.compare(accessorBuild, tIndex, accessorProbe, i);
                if (c == 0) {
                    matchFound = true;
                    if (!appender.appendConcat(accessorBuild, tIndex, accessorProbe, i)) {
                        flushFrame(outBuffer, writer);
                        appender.reset(outBuffer, true);
                        if (!appender.appendConcat(accessorBuild, tIndex, accessorProbe, i)) {
                            throw new IllegalStateException();
                        }
                    }
                    if(isLeftOuter)
                    	leftOuterJoinBitSets.get(bIndex).set(tIndex);
                }
            } while (true);

            if (!matchFound && isRightOuter) {
                if (!appender.appendConcat(leftNullTupleBuild.getFieldEndOffsets(),
                        leftNullTupleBuild.getByteArray(), 0, leftNullTupleBuild.getSize(), accessorProbe, i)) {
                    flushFrame(outBuffer, writer);
                    appender.reset(outBuffer, true);
                    if (!appender.appendConcat(leftNullTupleBuild.getFieldEndOffsets(),
                            leftNullTupleBuild.getByteArray(), 0, leftNullTupleBuild.getSize(), accessorProbe, i)) {
                        throw new IllegalStateException();
                    }
                }
            }
        }
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        if(isLeftOuter) {
	        int bIndex = -1;
	        BitSet joinBitSet;
	        Iterator<BitSet> bitSetsIterator = leftOuterJoinBitSets.iterator();
	        while(bitSetsIterator.hasNext()) {
	        	bIndex++;
	        	joinBitSet = bitSetsIterator.next();
                accessorBuild.reset(buffers.get(bIndex));
                for(int tIndex=joinBitSet.nextClearBit(0); tIndex>=0 && tIndex<accessorBuild.getTupleCount(); tIndex=joinBitSet.nextClearBit(tIndex+1)) {
                    if (!appender.appendConcat(accessorBuild, tIndex, rightNullTupleBuild.getFieldEndOffsets(),
                            rightNullTupleBuild.getByteArray(), 0, rightNullTupleBuild.getSize())) {
                        flushFrame(outBuffer, writer);
                        appender.reset(outBuffer, true);
	                    if (!appender.appendConcat(accessorBuild, tIndex, rightNullTupleBuild.getFieldEndOffsets(),
	                            rightNullTupleBuild.getByteArray(), 0, rightNullTupleBuild.getSize())) {
                            throw new IllegalStateException();
                        }
                    }
                }
	        }
        }
        if (appender.getTupleCount() > 0) {
            flushFrame(outBuffer, writer);
        }
    }

    private void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        writer.nextFrame(buffer);
        buffer.position(0);
        buffer.limit(buffer.capacity());
    }

    private static class Link {
        private static final int INIT_POINTERS_SIZE = 8;

        long[] pointers;
        int size;

        Link() {
            pointers = new long[INIT_POINTERS_SIZE];
            size = 0;
        }

        void add(long pointer) {
            if (size >= pointers.length) {
                pointers = Arrays.copyOf(pointers, pointers.length * 2);
            }
            pointers[size++] = pointer;
        }
    }
}