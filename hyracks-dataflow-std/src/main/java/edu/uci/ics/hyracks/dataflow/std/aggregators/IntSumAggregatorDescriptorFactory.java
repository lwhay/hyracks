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
package edu.uci.ics.hyracks.dataflow.std.aggregators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.DataOutput;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * @author jarodwen
 *
 */
public class IntSumAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;

    private final int aggField;
    private int outField = -1;

    public IntSumAggregatorDescriptorFactory(int aggField) {
        this.aggField = aggField;
    }

    public IntSumAggregatorDescriptorFactory(int aggField, int outField) {
        this.aggField = aggField;
        this.outField = outField;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksStageletContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int[] keyFields) throws HyracksDataException {

        final ArrayTupleBuilder tb = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        if (this.outField < 0) {
            this.outField = keyFields.length;
        }

        return new IAggregatorDescriptor() {

            @Override
            public boolean init(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                // Construct the tuple using keys and sum value
                tb.reset();
                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, keyFields[i]);
                }
                // Insert the aggregation value
                tb.addField(accessor, tIndex, aggField);

                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    return false;
                }
                return true;
            }

            @Override
            public void close() {
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
                    throws HyracksDataException {
                int sum = 0;
                int tupleOffset = accessor1.getTupleStartOffset(tIndex1);
                int fieldCount = accessor1.getFieldCount();
                int fieldStart = accessor1.getFieldStartOffset(tIndex1, aggField);
                sum += IntegerSerializerDeserializer.getInt(accessor1.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
                tupleOffset = accessor2.getTupleStartOffset(tIndex2);
                fieldCount = accessor2.getFieldCount();
                fieldStart = accessor2.getFieldStartOffset(tIndex2, outField);
                sum += IntegerSerializerDeserializer.getInt(accessor2.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
                // Update the value of tuple 2
                ByteBuffer buf = accessor2.getBuffer();
                buf.position(tupleOffset + 2 * fieldCount + fieldStart);
                buf.putInt(sum);
            }

            @Override
            public boolean outputPartialResult(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                // Construct the tuple using keys and sum value
                tb.reset();
                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, i);
                }
                // Insert the aggregation value
                tb.addField(accessor, tIndex, outField);
                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    return false;
                }
                return true;
            }

            @Override
            public boolean outputMergeResult(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                // Construct the tuple using keys and sum value
                tb.reset();
                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, i);
                }
                // Insert the aggregation value
                tb.addField(accessor, tIndex, outField);
                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    return false;
                }
                return true;
            }

            @Override
            public void merge(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
                    throws HyracksDataException {
                int sum = 0;
                // Get value from tuple 1
                int tupleOffset = accessor1.getTupleStartOffset(tIndex1);
                int fieldCount = accessor1.getFieldCount();
                int fieldStart = accessor1.getFieldStartOffset(tIndex1, outField);
                sum += IntegerSerializerDeserializer.getInt(accessor1.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
                // Get value from tuple 2
                tupleOffset = accessor2.getTupleStartOffset(tIndex2);
                fieldCount = accessor2.getFieldCount();
                fieldStart = accessor2.getFieldStartOffset(tIndex2, outField);
                sum += IntegerSerializerDeserializer.getInt(accessor2.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);
                // Update the value of tuple 2
                ByteBuffer buf = accessor2.getBuffer();
                buf.position(tupleOffset + 2 * fieldCount + fieldStart);
                buf.putInt(sum);
            }

            @Override
            public void getInitValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                int sum = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                sum += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);

                try {
                    dataOutput.writeInt(sum);
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }

            @Override
            public void getPartialOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                int sum = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                sum += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);

                try {
                    dataOutput.writeInt(sum);
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }
            
            @Override
            public void getMergeOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                int sum = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                sum += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount
                        + fieldStart);

                try {
                    dataOutput.writeInt(sum);
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }
        };
    }
}
