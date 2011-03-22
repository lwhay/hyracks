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

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * @author jarodwen
 *
 */
public class MultiAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    
    private final IAggregatorDescriptorFactory[] aggregatorFactories;

    public MultiAggregatorDescriptorFactory(IAggregatorDescriptorFactory[] aggregatorFactories) {
        this.aggregatorFactories = aggregatorFactories;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory#createAggregator(edu.uci.ics.hyracks.api.context.IHyracksStageletContext, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int[])
     */
    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksStageletContext ctx,
            final RecordDescriptor inRecordDescriptor, final RecordDescriptor outRecordDescriptor, final int[] keyFields)
            throws HyracksDataException {

        final ArrayTupleBuilder tb = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        final IAggregatorDescriptor[] aggregators = new IAggregatorDescriptor[this.aggregatorFactories.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = aggregatorFactories[i].createAggregator(ctx, inRecordDescriptor, outRecordDescriptor,
                    keyFields);
        }

        return new IAggregatorDescriptor() {

            @Override
            public boolean init(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                tb.reset();
                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, keyFields[i]);
                }
                DataOutput dos = tb.getDataOutput();
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].getInitValue(accessor, tIndex, dos);
                    tb.addFieldEndOffset();
                }
                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    return false;
                }
                return true;
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].aggregate(accessor1, tIndex1, accessor2, tIndex2);
                }
            }

            @Override
            public void merge(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].merge(accessor1, tIndex1, accessor2, tIndex2);
                }
            }

            @Override
            public boolean outputPartialResult(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
                    throws HyracksDataException {
                // Construct the tuple using keys and sum value
                tb.reset();
                for (int i = 0; i < keyFields.length; i++) {
                    tb.addField(accessor, tIndex, i);
                }
                DataOutput dos = tb.getDataOutput();
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].getPartialOutputValue(accessor, tIndex, dos);
                    tb.addFieldEndOffset();
                }
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
                DataOutput dos = tb.getDataOutput();
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].getMergeOutputValue(accessor, tIndex, dos);
                    tb.addFieldEndOffset();
                }
                // Write the tuple out
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    return false;
                }
                return true;
            }

            @Override
            public void close() {
                for(int i = 0; i < aggregators.length; i++){
                    aggregators[i].close();
                }
            }

            @Override
            public void getInitValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].getInitValue(accessor, tIndex, dataOutput);
                }
            }

            @Override
            public void getPartialOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].getPartialOutputValue(accessor, tIndex, dataOutput);
                }
            }
            
            @Override
            public void getMergeOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
                    throws HyracksDataException {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].getMergeOutputValue(accessor, tIndex, dataOutput);
                }
            }

        };
    }

}
