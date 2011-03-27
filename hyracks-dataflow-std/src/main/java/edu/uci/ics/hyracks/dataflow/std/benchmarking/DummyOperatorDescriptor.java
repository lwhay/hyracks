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
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

/**
 * @author jarodwen
 *
 */
public class DummyOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    public DummyOperatorDescriptor(JobSpecification spec) {
        super(spec, 0, 0);
        // TODO Auto-generated constructor stub
    }

    private static final long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.IActivityNode#createPushRuntime(edu.uci.ics.hyracks.api.context.IHyracksStageletContext, edu.uci.ics.hyracks.api.job.IOperatorEnvironment, edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int, int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        // TODO Auto-generated method stub
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            
            @Override
            public void open() throws HyracksDataException {
                // do nothing
            }
            
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // do nothing
            }
            
            @Override
            public void flush() throws HyracksDataException {
                // do nothing
            }
            
            @Override
            public void close() throws HyracksDataException {
                // do nothing
            }
        };
    }

}
