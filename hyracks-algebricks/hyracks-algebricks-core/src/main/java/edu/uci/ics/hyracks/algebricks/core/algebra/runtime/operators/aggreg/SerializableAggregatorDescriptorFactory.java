package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.aggreg;

import java.io.DataOutput;

import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.ISerializableAggregateFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.ISerializableAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class SerializableAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private ISerializableAggregateFunctionFactory[] aggFactories;

    public SerializableAggregatorDescriptorFactory(ISerializableAggregateFunctionFactory[] aggFactories) {
        this.aggFactories = aggFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, final int[] keyFieldsInPartialResults)
            throws HyracksDataException {

        final int[] keys = keyFields;
        final ArrayTupleBuilder initLengthCalBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        /**
         * one IAggregatorDescriptor instance per Gby operator
         */
        return new IAggregatorDescriptor() {
            private FrameTupleReference ftr = new FrameTupleReference();
            private ISerializableAggregateFunction[] aggs = new ISerializableAggregateFunction[aggFactories.length];
            private int offsetFieldIndex = keys.length;
            private int stateFieldLength[] = new int[aggFactories.length];

            @Override
            public AggregateState createAggregateStates() {
                return new AggregateState();
            }

            @Override
            public void init(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                    throws HyracksDataException {
                DataOutput output = tb.getDataOutput();
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        int begin = tb.getSize();
                        if (aggs[i] == null) {
                            aggs[i] = aggFactories[i].createAggregateFunction();
                        }
                        aggs[i].init(output);
                        tb.addFieldEndOffset();
                        stateFieldLength[i] = tb.getSize() - begin;
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }

                // doing initial aggregate
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        byte[] data = tb.getByteArray();
                        int prevFieldPos = i + keys.length - 1;
                        int start = prevFieldPos >= 0 ? tb.getFieldEndOffsets()[prevFieldPos] : 0;
                        aggs[i].step(ftr, data, start, stateFieldLength[i]);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                int stateTupleStart = stateAccessor.getTupleStartOffset(stateTupleIndex);
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        byte[] data = stateAccessor.getBuffer().array();
                        int start = stateAccessor.getFieldStartOffset(stateTupleIndex, i + keys.length)
                                + stateTupleStart;
                        aggs[i].step(ftr, data, start, stateFieldLength[i]);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finishPartial(data, start, stateFieldLength[i], tb.getDataOutput());
                        start += stateFieldLength[i];
                        tb.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void outputFinalResult(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finish(data, start, stateFieldLength[i], tb.getDataOutput());
                        start += stateFieldLength[i];
                        tb.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void reset() {

            }

            @Override
            public void close() {
                reset();
            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tupleBuilder, byte[] buf, int tupleStart,
                    int tupleLength, int fieldCount, int fieldSlotLength, AggregateState state)
                    throws HyracksDataException {
                int fieldOffset = tupleStart + fieldSlotLength * fieldCount;
                for (int i = 0; i < keys.length; i++) {
                    fieldOffset += IntegerSerializerDeserializer.getInt(buf, tupleStart + i * fieldSlotLength);
                }
                int refOffset = tupleStart + fieldSlotLength + fieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finishPartial(buf, start, stateFieldLength[i], tupleBuilder.getDataOutput());
                        start += stateFieldLength[i];
                        tupleBuilder.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void outputFinalResult(ArrayTupleBuilder tupleBuilder, byte[] buf, int tupleStart, int tupleLength,
                    int fieldCount, int fieldSlotLength, AggregateState state) throws HyracksDataException {
                int fieldOffset = tupleStart + fieldSlotLength * fieldCount;
                for (int i = 0; i < keys.length; i++) {
                    fieldOffset += IntegerSerializerDeserializer.getInt(buf, tupleStart + i * fieldSlotLength);
                }
                int refOffset = tupleStart + fieldSlotLength + fieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finish(buf, start, stateFieldLength[i], tupleBuilder.getDataOutput());
                        start += stateFieldLength[i];
                        tupleBuilder.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public int getInitSize(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                initLengthCalBuilder.reset();
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        int begin = initLengthCalBuilder.getSize();
                        if (aggs[i] == null) {
                            aggs[i] = aggFactories[i].createAggregateFunction();
                        }
                        aggs[i].init(initLengthCalBuilder.getDataOutput());
                        initLengthCalBuilder.addFieldEndOffset();
                        stateFieldLength[i] = initLengthCalBuilder.getSize() - begin;
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }

                // doing initial aggregate
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        byte[] data = initLengthCalBuilder.getByteArray();
                        int prevFieldPos = i + keys.length - 1;
                        int start = prevFieldPos >= 0 ? initLengthCalBuilder.getFieldEndOffsets()[prevFieldPos] : 0;
                        aggs[i].step(ftr, data, start, stateFieldLength[i]);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return initLengthCalBuilder.getFieldEndOffsets().length * 4 + initLengthCalBuilder.getSize();
            }

            @Override
            public int getFieldCount() {
                return aggs.length;
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length,
                    AggregateState state) throws HyracksDataException {
                // TODO Auto-generated method stub
                
            }

        };
    }
}
