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
package edu.uci.ics.hyracks.dataflow.std.benchmarking;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * Pass the input data directly to the down-stream operators
 *
 */
public class DummyDataProcessingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    
    private static final long serialVersionUID = 1L;

    public DummyDataProcessingOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.IActivityNode#createPushRuntime(edu.uci.ics.hyracks.api.context.IHyracksStageletContext, edu.uci.ics.hyracks.api.job.IOperatorEnvironment, edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int, int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        
        final FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptors[0]);
        final ByteBuffer outFrame = ctx.allocateFrame();
        final FrameTupleAppender outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
        outFrameAppender.reset(outFrame, true);
        
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            
            @Override
            public void open() throws HyracksDataException {
                writer.open();
            }
            
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for(int i = 0; i < tupleCount; i++){
                    if(!outFrameAppender.append(accessor, i)){
                        FrameUtils.flushFrame(outFrame, writer);
                        outFrameAppender.reset(outFrame, true);
                        if(!outFrameAppender.append(accessor, i)){
                            throw new HyracksDataException("Failed to append new tuple to the output frame!");
                        }
                    }
                }
            }
            
            @Override
            public void flush() throws HyracksDataException {
                
            }
            
            @Override
            public void close() throws HyracksDataException {
                if(outFrameAppender.getTupleCount() > 0){
                    FrameUtils.flushFrame(outFrame, writer);
                    outFrameAppender.reset(outFrame, true);
                }
                writer.close();
            }
        };
    }

}
