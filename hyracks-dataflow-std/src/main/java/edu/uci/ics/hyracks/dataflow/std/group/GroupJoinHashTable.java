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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class GroupJoinHashTable{
	
//	private final File outFile;
//	private PrintWriter pw;

    private static final int INIT_AGG_STATE_SIZE = 8;
    private final IHyracksTaskContext ctx;

    private final List<ByteBuffer> buffers;
    /**
     * Aggregate states: a list of states for all groups maintained in the main
     * memory.
     */
    private AggregateState[] aggregateStates;
    private int accumulatorSize;

    private int lastBIndex;
    private final int[] storedKeys;
    private final int[] keys;
    private final IBinaryComparator[] comparators;
    private final ITuplePartitionComputer tpc;
    private final IAggregatorDescriptor aggregator;

    private final FrameTupleAppender appender;

    private final FrameTupleAccessor storedKeysAccessor;

    private final ArrayTupleBuilder stateTupleBuilder, outputTupleBuilder;
    private final ITuplePartitionComputer tpc1;
    protected final FrameTuplePairComparator ftpc1;
    private final int tableSize;
    private final ISerializableTable hashTable;
    private final INullWriter[] nullWriters;
    
    public GroupJoinHashTable(IHyracksTaskContext ctx, int[] gFields, int[] jFields,
    		IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFactory gByTpc0, ITuplePartitionComputerFactory gByTpc1,
    		IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, 
            INullWriter[] nullWriters1, int tableSize) throws HyracksDataException {
    	
    	this.ctx = ctx;
    	
        buffers = new ArrayList<ByteBuffer>();

        RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        
        this.keys = gFields;
        this.storedKeys = new int[gFields.length];
        
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[gFields.length + 1];
        for (int i = 0; i < gFields.length; ++i) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[gFields[i]];
        }
        storedKeySerDeser[gFields.length] = dummyRecordDescriptor.getFields()[0];

        RecordDescriptor storedKeysRecordDescriptor = new RecordDescriptor(storedKeySerDeser);
        storedKeysAccessor = new FrameTupleAccessor(ctx.getFrameSize(), storedKeysRecordDescriptor);
        
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        tpc = gByTpc0.createPartitioner();

        int[] keyFieldsInPartialResults = new int[gFields.length];
        for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
            keyFieldsInPartialResults[i] = i;
        }

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDescriptor, outRecordDescriptor, gFields,
                keyFieldsInPartialResults);

        this.aggregateStates = new AggregateState[INIT_AGG_STATE_SIZE];
        accumulatorSize = 0;

        lastBIndex = -1;

        appender = new FrameTupleAppender(ctx.getFrameSize());

        addNewBuffer();

        stateTupleBuilder = new ArrayTupleBuilder(gFields.length + 1);
        outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        tpc1 = gByTpc1.createPartitioner();
        ftpc1 = new FrameTuplePairComparator(storedKeys, jFields, comparators);
        this.tableSize = tableSize;
        
        this.nullWriters = nullWriters1;
        
        hashTable = new SerializableHashTable(tableSize, ctx);
    }

    private void addNewBuffer() {
        ByteBuffer buffer = ctx.allocateFrame();
        buffer.position(0);
        buffer.limit(buffer.capacity());
        buffers.add(buffer);
        appender.reset(buffer, true);
        ++lastBIndex;
    }
    
    private void updateAggregateIndex(FrameTupleAccessor accessor0, TuplePointer tp, int idx) {
    	
            int tStart = accessor0.getTupleStartOffset(tp.tupleIndex);
            int fStartOffset = accessor0.getFieldSlotsLength() + tStart;

            int fStart = accessor0.getFieldStartOffset(tp.tupleIndex, keys.length);
            int fEnd = accessor0.getFieldEndOffset(tp.tupleIndex, keys.length);
            int fLen = fEnd - fStart;
            
            byte[] b = new byte[4];
            for(int i=3; i>=0; i--) {
            	b[i] = (byte) (0xff & idx);
            	idx = idx >> 8;
            }
            
            System.arraycopy(b, 0, accessor0.getBuffer().array(), fStart + fStartOffset, fLen);
            buffers.set(tp.frameIndex, accessor0.getBuffer());
    }

    private int getAggregateIndex(FrameTupleAccessor accessor0, int tIndex) {
    	
    	int idx = 0;
    	
        int tStart = accessor0.getTupleStartOffset(tIndex);
        int fStartOffset = accessor0.getFieldSlotsLength() + tStart;
        int fieldCount = accessor0.getFieldCount();
        int fStart = accessor0.getFieldStartOffset(tIndex, fieldCount - 1);

    	idx = accessor0.getBuffer().getInt(fStart + fStartOffset);
        return idx;
    }

    public void build(FrameTupleAccessor accessor, ByteBuffer buffer) throws HyracksDataException, IOException {
        accessor.reset(buffer);
        int tCount = accessor.getTupleCount();
        int entry;
        TuplePointer storedTuplePointer = new TuplePointer();

        for (int tIndex = 0; tIndex < tCount; ++tIndex) {
        	entry = tpc.partition(accessor, tIndex, tableSize);
        	
            stateTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                stateTupleBuilder.addField(accessor, tIndex, keys[k]);
            }
            stateTupleBuilder.getDataOutput().writeInt(-1);
            stateTupleBuilder.addFieldEndOffset();

            if (!appender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                    stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                addNewBuffer();
                if (!appender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                        stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                    throw new HyracksDataException("Cannot init the aggregate state in a single frame.");
                }
            }

            storedTuplePointer.frameIndex = lastBIndex;
            storedTuplePointer.tupleIndex = appender.getTupleCount() - 1;
            hashTable.insert(entry, storedTuplePointer);
        }
    }
    
	public void insert(FrameTupleAccessor accessor, ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount0 = accessor.getTupleCount();
        int entry, saIndex, offset;
        boolean foundGroup = false;
        TuplePointer storedTuplePointer = new TuplePointer();
        
        for (int tIndex = 0; tIndex < tupleCount0; ++tIndex) {
		    entry = tpc1.partition(accessor, tIndex, tableSize);
		    
            foundGroup = false;
            offset = 0;
            do {
                hashTable.getTuplePointer(entry, offset++, storedTuplePointer);
                if (storedTuplePointer.frameIndex < 0)
                    break;
                storedKeysAccessor.reset(buffers.get(storedTuplePointer.frameIndex));
                
			    int c = ftpc1.compare(storedKeysAccessor, storedTuplePointer.tupleIndex, accessor, tIndex);
                if (c == 0) {
                    foundGroup = true;
                    break;
                }
            } while (true);

            if (foundGroup) {
            	saIndex = getAggregateIndex(storedKeysAccessor, storedTuplePointer.tupleIndex);
            	AggregateState state;
            	
            	if(saIndex < 0) {
                	saIndex = accumulatorSize++;
                	state = aggregator.createAggregateStates();
                	aggregator.init(stateTupleBuilder, accessor, tIndex, state);
                	
                	if (accumulatorSize >= aggregateStates.length) {
                		aggregateStates = Arrays.copyOf(aggregateStates, aggregateStates.length * 2);
                	}

                	updateAggregateIndex(storedKeysAccessor, storedTuplePointer, saIndex);
            	}
            	else {
            		state = aggregateStates[saIndex];
                    aggregator.aggregate(accessor, tIndex, storedKeysAccessor, storedTuplePointer.tupleIndex, state);
            	}

            	aggregateStates[saIndex] = state;
            }
        }
	}
	
	public void write(IFrameWriter writer) throws HyracksDataException {
		int saIndex;
		ByteBuffer buffer = ctx.allocateFrame();
		appender.reset(buffer, true);
		
		
		for (int i = 0; i < buffers.size(); ++i) {
			storedKeysAccessor.reset(buffers.get(i));

			for (int tIndex = 0; tIndex < storedKeysAccessor.getTupleCount(); ++tIndex) {
				outputTupleBuilder.reset();
				for (int j = 0; j < storedKeys.length; j++) {
					outputTupleBuilder.addField(storedKeysAccessor, tIndex, storedKeys[j]);
				}
				saIndex = getAggregateIndex(storedKeysAccessor, tIndex);
				
				if(saIndex < 0) {
					for (int k = 0; k < nullWriters.length; k++) {
			            nullWriters[k].writeNull(outputTupleBuilder.getDataOutput());
			            outputTupleBuilder.addFieldEndOffset();
					}
				}
				else {
					aggregator.outputFinalResult(outputTupleBuilder, storedKeysAccessor, tIndex, aggregateStates[saIndex]);
				}
				
				if (!appender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
						outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
					writer.nextFrame(buffer);
					appender.reset(buffer, true);
					if (!appender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
							outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
						throw new HyracksDataException("Cannot write the aggregation output into a frame.");
					}
				}

			}

		}

		if (appender.getTupleCount() != 0) {
			writer.nextFrame(buffer);
		}
	}
	
    void close() throws HyracksDataException {
        for (AggregateState aState : aggregateStates) {
            aState.close();
        }
    }
}