package edu.uci.ics.genomix.util;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class StatSumAggregateFactory implements IAggregatorDescriptorFactory {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public class DistributeAggregatorDescriptor implements
			IAggregatorDescriptor {

		@Override
		public AggregateState createAggregateStates() {
			// TODO Auto-generated method stub
			return null;
		}

		protected int getCount(IFrameTupleAccessor accessor, int tIndex) {
			int tupleOffset = accessor.getTupleStartOffset(tIndex);
			int fieldStart = accessor.getFieldStartOffset(tIndex, 1);
			int countoffset = tupleOffset + accessor.getFieldSlotsLength()
					+ fieldStart;
			byte[] data = accessor.getBuffer().array();
			return IntegerSerializerDeserializer.getInt(data, countoffset);
		}

		@Override
		public void init(ArrayTupleBuilder tupleBuilder,
				IFrameTupleAccessor accessor, int tIndex, AggregateState state)
				throws HyracksDataException {
			int count = getCount(accessor, tIndex);

			DataOutput fieldOutput = tupleBuilder.getDataOutput();
			try {
				fieldOutput.writeInt(count);
				tupleBuilder.addFieldEndOffset();
			} catch (IOException e) {
				throw new HyracksDataException(
						"I/O exception when initializing the aggregator.");
			}
		}

		@Override
		public void reset() {
			// TODO Auto-generated method stub

		}

		@Override
		public void aggregate(IFrameTupleAccessor accessor, int tIndex,
				IFrameTupleAccessor stateAccessor, int stateTupleIndex,
				AggregateState state) throws HyracksDataException {
			int count = getCount(accessor, tIndex);

			int statetupleOffset = stateAccessor
					.getTupleStartOffset(stateTupleIndex);
			int countfieldStart = stateAccessor.getFieldStartOffset(
					stateTupleIndex, 1);
			int countoffset = statetupleOffset
					+ stateAccessor.getFieldSlotsLength() + countfieldStart;

			byte[] data = stateAccessor.getBuffer().array();
			count += IntegerSerializerDeserializer.getInt(data, countoffset);
			IntegerSerializerDeserializer.putInt(count, data, countoffset);
		}

		@Override
		public void outputPartialResult(ArrayTupleBuilder tupleBuilder,
				IFrameTupleAccessor accessor, int tIndex, AggregateState state)
				throws HyracksDataException {
			int count = getCount(accessor, tIndex);
			DataOutput fieldOutput = tupleBuilder.getDataOutput();
			try {
				fieldOutput.writeInt(count);
				tupleBuilder.addFieldEndOffset();
			} catch (IOException e) {
				throw new HyracksDataException(
						"I/O exception when writing aggregation to the output buffer.");
			}

		}

		@Override
		public void outputFinalResult(ArrayTupleBuilder tupleBuilder,
				IFrameTupleAccessor accessor, int tIndex, AggregateState state)
				throws HyracksDataException {
			outputPartialResult(tupleBuilder, accessor, tIndex, state);

		}

		@Override
		public void close() {
			// TODO Auto-generated method stub

		}

	}

	@Override
	public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx,
			RecordDescriptor inRecordDescriptor,
			RecordDescriptor outRecordDescriptor, int[] keyFields,
			int[] keyFieldsInPartialResults) throws HyracksDataException {
		// TODO Auto-generated method stub
		return new DistributeAggregatorDescriptor();
	}

}
