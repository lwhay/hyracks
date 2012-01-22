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
import java.util.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;

public class GroupJoinHashTable extends GroupingHashTable{
    private static class Link {
        private static final int INIT_POINTERS_SIZE = 12;

        int[] pointers;
        int size;

        Link() {
            pointers = new int[INIT_POINTERS_SIZE];
            size = 0;
        }

        void add(int bufferIdx, int tIndex, int accumulatorIdx, int flag) {
            while (size + 4 > pointers.length) {
                pointers = Arrays.copyOf(pointers, pointers.length * 2);
            }
            pointers[size++] = bufferIdx;
            pointers[size++] = tIndex;
            pointers[size++] = accumulatorIdx;
            pointers[size++] = flag;
        }
    }

    private final Link[] table;
    private final int[] gFields;
    private final INullWriter[] nullWriters;
    private final ITuplePartitionComputer tpc1;
    protected final FrameTuplePairComparator ftpc1;

    public GroupJoinHashTable(IHyracksTaskContext ctx, int[] gFields, int[] jFields,
    		IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFactory gByTpc0, ITuplePartitionComputerFactory gByTpc1,
            IAccumulatingAggregatorFactory aggregatorFactory, RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, 
            INullWriter[] nullWriters1, int tableSize) {
       	super( ctx, jFields, comparatorFactories, gByTpc0,  aggregatorFactory,
             inRecordDescriptor,  outRecordDescriptor,  tableSize);
        tpc1 = gByTpc1.createPartitioner();
       	table = new Link[tableSize];
        ftpc1 = new FrameTuplePairComparator(storedKeys, jFields, comparators);
        this.gFields = gFields;
        this.nullWriters = nullWriters1;
    }

    public void build(FrameTupleAccessor accessor, ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tCount = accessor.getTupleCount();
        for (int tIndex = 0; tIndex < tCount; ++tIndex) {
	        int entry = tpc.partition(accessor, tIndex, table.length);
	        Link link = table[entry];
	        if (link == null) {
	            link = table[entry] = new Link();
	        }
	        IAccumulatingAggregator aggregator = null;

            // Insert a new entry.
	            if (!appender.appendProjection(accessor, tIndex, gFields)) {
	                addNewBuffer();
	                if (!appender.appendProjection(accessor, tIndex, gFields)) {
	                    throw new IllegalStateException();
	                }
	            }
	            int sbIndex = lastBIndex;
	            int stIndex = appender.getTupleCount() - 1;
	            if (accumulatorSize >= accumulators.length) {
	                accumulators = Arrays.copyOf(accumulators, accumulators.length * 2);
	            }
	            int saIndex = accumulatorSize++;
	            aggregator = accumulators[saIndex] = aggregatorFactory.createAggregator(ctx, inRecordDescriptor,
	                    outRecordDescriptor);
	            link.add(sbIndex, stIndex, saIndex, 0);
        }
    }
    
	public void insert(FrameTupleAccessor accessor, ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount0 = accessor.getTupleCount();
        for (int tIndex = 0; tIndex < tupleCount0; ++tIndex) {
		    int entry = tpc1.partition(accessor, tIndex, table.length);
		    Link link = table[entry];
		    if (link == null) {
		    	continue;
		    }
		    IAccumulatingAggregator aggregator = null;
		    int i = 0;
		    for (; i < link.size; i += 4) {
			    int sbIndex = link.pointers[i];
			    int stIndex = link.pointers[i + 1];
			    int saIndex = link.pointers[i + 2];
			    storedKeysAccessor.reset(buffers.get(sbIndex));

			    int c = ftpc1.compare(storedKeysAccessor, stIndex, accessor, tIndex);
			    if (c == 0) {
			        aggregator = accumulators[saIndex];
			        break;
			    }
			}
		
		    if (aggregator != null){
		    	if (link.pointers[i + 3] == 0){
		            aggregator.init(accessor, tIndex);
		    		link.pointers[i + 3] = 1;
		    	}
		        aggregator.accumulate(accessor, tIndex);
		    }
        }
	}
	
    @Override
	public void write(IFrameWriter writer) throws HyracksDataException {
	    ByteBuffer buffer = ctx.allocateFrame();
	    appender.reset(buffer, true);
	    for (int i = 0; i < table.length; ++i) {
	        Link link = table[i];
	        if (link != null) {
	            for (int j = 0; j < link.size; j += 4) {
	                int bIndex = link.pointers[j];
	                int tIndex = link.pointers[j + 1];
	                int aIndex = link.pointers[j + 2];
	                boolean writeNull = link.pointers[j + 3] == 0 ? true : false;
	                ByteBuffer keyBuffer = buffers.get(bIndex);
	                storedKeysAccessor.reset(keyBuffer);
	                IAccumulatingAggregator aggregator = accumulators[aIndex];
	                while (!aggregator.output(appender, storedKeysAccessor, tIndex, storedKeys, 
	                		writeNull, nullWriters)) {
	                    flushFrame(appender, writer);
	                }
	            }
	        }
	    }
	    if (appender.getTupleCount() != 0) {
	        flushFrame(appender, writer);
	    }
	}

}