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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.DataOutput;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * @author jarodwen
 */
public class ConcatAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;

    private static final int INIT_ACCUMULATORS_SIZE = 8;

    private final int concatField;
    private int outField = -1;

    public ConcatAggregatorDescriptorFactory(int concatField) {
        this.concatField = concatField;
    }

    public ConcatAggregatorDescriptorFactory(int concatField, int outField) {
        this.concatField = concatField;
        this.outField = outField;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksStageletContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int[] keyFields) throws HyracksDataException {

        if (this.outField < 0)
            this.outField = keyFields.length;

        final ArrayTupleBuilder tb = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        return new IAggregatorDescriptor() {

            byte[][] buf = new byte[INIT_ACCUMULATORS_SIZE][];

            int currentAggregatorIndex = -1;
            int aggregatorCount = 0;

            @Override
            public void merge(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
                    throws HyracksDataException {
                int tupleOffset = accessor2.getTupleStartOffset(tIndex2);
                int fieldCount = accessor2.getFieldCount();
                int fieldStart = accessor2.getFieldStartOffset(tIndex2, outField);
                int fieldLength = accessor2.getFieldLength(tIndex2, outField);

                // FIXME Should be done in binary way
                StringBuilder sbder = new StringBuilder();
                sbder.append(UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(accessor2.getBuffer().array(), tupleOffset + 2 * fieldCount
                                + fieldStart, fieldLength))));

                // Get the new data
                tupleOffset = accessor1.getTupleStartOffset(tIndex1);
                fieldCount = accessor1.getFieldCount();
                fieldStart = accessor1.getFieldStartOffset(tIndex1, outField);
                fieldLength = accessor1.getFieldLength(tIndex1, outField);
                sbder.append(UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(accessor1.getBuffer().array(), tupleOffset + 2 * fieldCount
                                + fieldStart, fieldLength))));

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(sbder.toString(), new DataOutputStream(baos));
                ByteBuffer wrapBuf = accessor2.getBuffer();
                wrapBuf.position(tupleOffset + 2 * fieldCount + fieldStart);
                wrapBuf.put(baos.toByteArray());
            }

            @Override
            public boolean init(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                tb.reset();
                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, keyFields[i]);
                }
                // Initialize the aggregation value
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int tupleEndOffset = accessor.getTupleEndOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, concatField);
                int appendOffset = tupleOffset + 2 * fieldCount + fieldStart;
                // Get the initial value
                currentAggregatorIndex++;
                if (currentAggregatorIndex >= buf.length) {
                    byte[][] newBuf = new byte[buf.length * 2][];
                    for (int i = 0; i < buf.length; i++) {
                        newBuf[i] = buf[i];
                    }
                    this.buf = newBuf;
                }
                buf[currentAggregatorIndex] = new byte[tupleEndOffset - appendOffset + 1];
                System.arraycopy(accessor.getBuffer().array(), appendOffset, buf[currentAggregatorIndex], 0,
                        tupleEndOffset - appendOffset + 1);
                // Update the aggregator index
                aggregatorCount++;

                tb.addField(IntegerSerializerDeserializer.INSTANCE, currentAggregatorIndex);
                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {

                    return false;
                }
                return true;
            }

            @Override
            public void close() {
                currentAggregatorIndex = -1;
                aggregatorCount = 0;
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
                    throws HyracksDataException {
                int tupleOffset = accessor2.getTupleStartOffset(tIndex2);
                int fieldCount = accessor2.getFieldCount();
                int fieldStart = accessor2.getFieldStartOffset(tIndex2, outField);
                int refIndex = IntegerSerializerDeserializer.getInt(accessor2.getBuffer().array(), tupleOffset + 2
                        * fieldCount + fieldStart);
                // FIXME Should be done in binary way
                StringBuilder sbder = new StringBuilder();
                sbder.append(UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(buf[refIndex]))));
                // Get the new data
                tupleOffset = accessor1.getTupleStartOffset(tIndex1);
                fieldCount = accessor1.getFieldCount();
                fieldStart = accessor1.getFieldStartOffset(tIndex1, concatField);
                int fieldLength = accessor1.getFieldLength(tIndex1, concatField);
                sbder.append(UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(accessor1.getBuffer().array(), tupleOffset + 2 * fieldCount
                                + fieldStart, fieldLength))));

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(sbder.toString(), new DataOutputStream(baos));
                buf[refIndex] = baos.toByteArray();
            }

            @Override
            public boolean outputMergeResult(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                // Construct the tuple using keys and sum value
                tb.reset();

                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, i);
                }
                tb.addField(accessor, tIndex, outField);
                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    return false;
                }
                return true;
            }

            @Override
            public boolean outputPartialResult(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                // Construct the tuple using keys and sum value
                tb.reset();

                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, i);
                }

                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                int refIndex = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset + 2
                        * fieldCount + fieldStart);

                tb.addField(buf[refIndex], 0, buf[refIndex].length);
                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    return false;
                }
                return true;
            }

            @Override
            public void getInitValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, concatField);
                int fieldLength = accessor.getFieldLength(tIndex, concatField);

                // Get the initial value
                currentAggregatorIndex++;
                if (currentAggregatorIndex >= buf.length) {
                    byte[][] newBuf = new byte[buf.length * 2][];
                    for (int i = 0; i < buf.length; i++) {
                        newBuf[i] = buf[i];
                    }
                    this.buf = newBuf;
                }
                buf[currentAggregatorIndex] = new byte[fieldLength];
                System.arraycopy(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount + fieldStart,
                        buf[currentAggregatorIndex], 0, fieldLength);
                // Update the aggregator index
                aggregatorCount++;

                try {
                    dataOutput.writeInt(currentAggregatorIndex);
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }

            @Override
            public void getMergeOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                int fieldLength = accessor.getFieldLength(tIndex, outField);
                try {
                    dataOutput.write(accessor.getBuffer().array(), tupleOffset + 2 * fieldCount + fieldStart,
                            fieldLength);
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }
            
            @Override
            public void getPartialOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldCount = accessor.getFieldCount();
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                int bufIndex = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset + 2
                        * fieldCount + fieldStart);
                try {
                    dataOutput.write(buf[bufIndex]);
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }
        };
    }
}
