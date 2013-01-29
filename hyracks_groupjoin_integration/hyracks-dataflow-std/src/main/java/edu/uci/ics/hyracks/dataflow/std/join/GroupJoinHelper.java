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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
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
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameConstants;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class GroupJoinHelper{
	
    private final IHyracksTaskContext ctx;

    private final ArrayList<ByteBuffer> buffers;
    private final ArrayList<ByteBuffer> stateBuffers;
    /**
     * Aggregate states: a list of states for all groups maintained in the main
     * memory.
     */
    private int lastBIndex;
    private int lastStateBIndex;
    private final int[] storedKeys;
    private final int[] keys;
    private final int[] decors;
    private final int[] keysAndDecors;
    private final IBinaryComparator[] comparators;
    private final ITuplePartitionComputer tpc;
    private final IAggregatorDescriptor aggregator;
    private final FrameTupleAppender appender, stateAppender;
    private final FrameTupleAccessor stateAccessor, storedKeysAccessor;
    private final ArrayTupleBuilder groupTupleBuilder, stateTupleBuilder, outputTupleBuilder, nullTupleBuilder;
    private final ITuplePartitionComputer tpc1;
    protected final FrameTuplePairComparator ftpc1;
    private final int tableSize;
    private final ISerializableTable hashTable;
    private final boolean isLeftOuter;
    private final INullWriter[] nullWriters;
    private AggregateState state;
    
    public GroupJoinHelper(IHyracksTaskContext ctx, int[] gFields, int[] jFields, int[] dFields,
    		IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFactory gByTpc0,
    		ITuplePartitionComputerFactory gByTpc1, IAggregatorDescriptorFactory aggregatorFactory,
    		RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, boolean isLeftOuter,
    		INullWriter[] nullWriters1, int tableSize) throws HyracksDataException {
    	
    	this.ctx = ctx;
    	
        buffers = new ArrayList<ByteBuffer>();
        stateBuffers = new ArrayList<ByteBuffer>();

        this.keys = gFields;
        this.storedKeys = new int[keys.length];
        this.decors = dFields;
        this.keysAndDecors = new int[keys.length + decors.length];
        
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keys.length + decors.length + 2];
        for (int i = 0; i < keys.length; ++i) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[keys[i]];
            keysAndDecors[i] = i;
        }
        for (int i = 0; i < decors.length; ++i) {
            keysAndDecors[keys.length + i] = keys.length + i;
            storedKeySerDeser[keys.length + i] = inRecordDescriptor.getFields()[decors[i]];
        }
        storedKeySerDeser[keys.length + decors.length] = IntegerSerializerDeserializer.INSTANCE;
        storedKeySerDeser[keys.length + decors.length + 1] = IntegerSerializerDeserializer.INSTANCE;

        RecordDescriptor storedKeysRecordDescriptor = new RecordDescriptor(storedKeySerDeser);
        storedKeysAccessor = new FrameTupleAccessor(ctx.getFrameSize(), storedKeysRecordDescriptor);
        
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        tpc = gByTpc0.createPartitioner();

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDescriptor, outRecordDescriptor, keys, storedKeys);
        
        stateAccessor = new FrameTupleAccessor(ctx.getFrameSize(), outRecordDescriptor);

        lastBIndex = -1;
        lastStateBIndex = -1;

        appender = new FrameTupleAppender(ctx.getFrameSize());
        stateAppender = new FrameTupleAppender(ctx.getFrameSize());

        addNewBuffer(false);
        addNewBuffer(true);

        groupTupleBuilder = new ArrayTupleBuilder(keysAndDecors.length + 2);
        stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length - decors.length);
        outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        tpc1 = gByTpc1.createPartitioner();
        ftpc1 = new FrameTuplePairComparator(storedKeys, jFields, comparators);
        this.tableSize = tableSize;
        
        this.isLeftOuter = isLeftOuter;
        this.nullWriters = nullWriters1;
        if (isLeftOuter) {
            nullTupleBuilder = new ArrayTupleBuilder(nullWriters1.length);
            DataOutput out = nullTupleBuilder.getDataOutput();
            for (int i = 0; i < nullWriters1.length; i++) {
                nullWriters1[i].writeNull(out);
                nullTupleBuilder.addFieldEndOffset();
            }
        } else {
            nullTupleBuilder = null;
        }

        
        hashTable = new SerializableHashTable(tableSize, ctx);
    }

    private void addNewBuffer(boolean isStateBuffer) {
        ByteBuffer buffer = ctx.allocateFrame();
        buffer.position(0);
        buffer.limit(buffer.capacity());
        if (!isStateBuffer) {
        	buffers.add(buffer);
        	appender.reset(buffer, true);
        	++lastBIndex;
        } else {
        	stateBuffers.add(buffer);
        	stateAppender.reset(buffer, true);
        	++lastStateBIndex;
        }
    }
    
    private void updateAggregateIndex(FrameTupleAccessor accessor0, TuplePointer tp1, TuplePointer tp2) {
    	
            int tStart = accessor0.getTupleStartOffset(tp1.tupleIndex);
            int fStartOffset = accessor0.getFieldSlotsLength() + tStart;

            int fieldCount = accessor0.getFieldCount();
            int fStart = accessor0.getFieldStartOffset(tp1.tupleIndex, fieldCount - 2);
            
            accessor0.getBuffer().putInt(fStart + fStartOffset, tp2.frameIndex);
            fStart = accessor0.getFieldStartOffset(tp1.tupleIndex, fieldCount - 1);
            accessor0.getBuffer().putInt(fStart + fStartOffset, tp2.tupleIndex);
    }

    private TuplePointer getAggregateIndex(FrameTupleAccessor accessor0, int tIndex) {
    	
    	TuplePointer tp = new TuplePointer();
    	
        int tStart = accessor0.getTupleStartOffset(tIndex);
        int fStartOffset = accessor0.getFieldSlotsLength() + tStart;
        int fieldCount = accessor0.getFieldCount();
        int fStart = accessor0.getFieldStartOffset(tIndex, fieldCount - 2);

    	tp.frameIndex = accessor0.getBuffer().getInt(fStart + fStartOffset);
        fStart = accessor0.getFieldStartOffset(tIndex, fieldCount - 1);
    	tp.tupleIndex = accessor0.getBuffer().getInt(fStart + fStartOffset);
        return tp;
    }
    
    public void getField(FrameTupleAccessor accessor, int tIndex, int fIndex) {
        int startOffset = accessor.getTupleStartOffset(tIndex);
        int fStartOffset = accessor.getFieldStartOffset(tIndex, fIndex);
        int fLen = accessor.getFieldEndOffset(tIndex, fIndex) - fStartOffset;
        System.out.print(" |");
        for (int i=startOffset + accessor.getFieldSlotsLength() + fStartOffset; i < startOffset + accessor.getFieldSlotsLength() + fStartOffset + fLen; i++)
        	System.out.print(" " + accessor.getBuffer().get(i));
    }


    public void build(FrameTupleAccessor accessor, ByteBuffer buffer) throws HyracksDataException, IOException {
        accessor.reset(buffer);
        int tCount = accessor.getTupleCount();
        int entry;
        TuplePointer storedTuplePointer = new TuplePointer();

        for (int tIndex = 0; tIndex < tCount; ++tIndex) {
        	entry = tpc.partition(accessor, tIndex, tableSize);
        	
            groupTupleBuilder.reset();
//            System.out.print("\nBTuple : " + entry);
            for (int k = 0; k < keys.length; k++) {
//                getField(accessor, tIndex, keys[k]);
                groupTupleBuilder.addField(accessor, tIndex, keys[k]);
            }
            for (int d = 0; d < decors.length; d++) {
//                getField(accessor, tIndex, decors[d]);
                groupTupleBuilder.addField(accessor, tIndex, decors[d]);
            }
            groupTupleBuilder.getDataOutput().writeInt(-1);
            groupTupleBuilder.addFieldEndOffset();
            groupTupleBuilder.getDataOutput().writeInt(-1);
            groupTupleBuilder.addFieldEndOffset();

            if (!appender.appendSkipEmptyField(groupTupleBuilder.getFieldEndOffsets(),
                    groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize())) {
                addNewBuffer(false);
                if (!appender.appendSkipEmptyField(groupTupleBuilder.getFieldEndOffsets(),
                        groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize())) {
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
        int entry, offset;
        boolean foundGroup = false;
        TuplePointer storedTuplePointer = new TuplePointer();
        TuplePointer stateTuplePointer = new TuplePointer();
    	state = aggregator.createAggregateStates();
        
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
//                System.out.print("\nITuple : " + entry);
//                getField(accessor, tIndex, storedKeys[0]);
            	stateTuplePointer = getAggregateIndex(storedKeysAccessor, storedTuplePointer.tupleIndex);
            	
            	if(stateTuplePointer.frameIndex < 0) {
            		outputTupleBuilder.reset();

                    for (int k = 0; k < keys.length; k++) {
                    	outputTupleBuilder.addField(storedKeysAccessor, storedTuplePointer.tupleIndex, storedKeys[k]);
                    }

                	aggregator.init(outputTupleBuilder, accessor, tIndex, state);

    				if (!stateAppender.append(outputTupleBuilder.getFieldEndOffsets(),
    						outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
    					addNewBuffer(true);
        				if (!stateAppender.append(outputTupleBuilder.getFieldEndOffsets(),
        						outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
    						throw new HyracksDataException("Cannot write the state into a frame.");
    					}
    				}

    				stateTuplePointer.frameIndex = lastStateBIndex;
    				stateTuplePointer.tupleIndex = stateAppender.getTupleCount() - 1;
                	updateAggregateIndex(storedKeysAccessor, storedTuplePointer, stateTuplePointer);
            	}
            	else {
            		stateAccessor.reset(stateBuffers.get(stateTuplePointer.frameIndex));
                    aggregator.aggregate(accessor, tIndex, stateAccessor, stateTuplePointer.tupleIndex, state);
//                    stateAccessor.prettyPrint();
            	}
            }
        }
	}
	
	public void write(IFrameWriter writer) throws HyracksDataException {
		ByteBuffer buffer = ctx.allocateFrame();
        ByteBuffer tmpBuffer = ctx.allocateFrame();
		appender.reset(buffer, true);
		TuplePointer stateTuplePointer = new TuplePointer();
		int currentStateBuffer = -1;
		boolean emitTuple;
		
		
		for (int i = 0; i < buffers.size(); ++i) {
			storedKeysAccessor.reset(buffers.get(i));

			for (int tIndex = 0; tIndex < storedKeysAccessor.getTupleCount(); ++tIndex) {
				stateTuplePointer = getAggregateIndex(storedKeysAccessor, tIndex);
				outputTupleBuilder.reset();
				emitTuple = false;

				if(stateTuplePointer.frameIndex > -1) {
					emitTuple = true;
					for (int j = 0; j < keysAndDecors.length; j++) {
						outputTupleBuilder.addField(storedKeysAccessor, tIndex, keysAndDecors[j]);
					}
					if (currentStateBuffer != stateTuplePointer.frameIndex) {
						stateAccessor.reset(stateBuffers.get(stateTuplePointer.frameIndex));
						currentStateBuffer = stateTuplePointer.frameIndex;
					}
					aggregator.outputFinalResult(outputTupleBuilder, stateAccessor, stateTuplePointer.tupleIndex, state);
				}
				else if(isLeftOuter) {
					emitTuple=true;
/*					for (int k = 0; k < nullWriters.length; k++) {
			            nullWriters[k].writeNull(outputTupleBuilder.getDataOutput());
			            outputTupleBuilder.addFieldEndOffset();
					}
*/			    	
//					tmpBuffer.clear();
					state = aggregator.createAggregateStates();
                	stateAppender.reset(tmpBuffer, true);
                	
            		outputTupleBuilder.reset();

                    for (int k = 0; k < storedKeys.length; k++) {
                    	outputTupleBuilder.addField(storedKeysAccessor, tIndex, storedKeys[k]);
                    }

                	aggregator.init(outputTupleBuilder, null, 0, state);

    				if (!stateAppender.append(outputTupleBuilder.getFieldEndOffsets(),
    						outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize()))
						throw new HyracksDataException("Cannot write the state into a frame.");

            		outputTupleBuilder.reset();

					for (int j = 0; j < keysAndDecors.length; j++) {
						outputTupleBuilder.addField(storedKeysAccessor, tIndex, keysAndDecors[j]);
					}
					
					stateAccessor.reset(tmpBuffer);
					currentStateBuffer = -1;

					aggregator.outputFinalResult(outputTupleBuilder, stateAccessor, 0, state);
				}
				
				if (emitTuple) {
					if (!appender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
							outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
						writer.nextFrame(buffer);
						appender.reset(buffer, true);
						if (!appender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
								outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
							throw new HyracksDataException("Cannot write groupjoin output into a frame.");
						}
					}
				}
			}
		}

		if (appender.getTupleCount() != 0) {
			writer.nextFrame(buffer);
		}
	}
	
    public void close() throws HyracksDataException {
    	buffers.clear();
    	stateBuffers.clear();
    	state = null;
    }
}