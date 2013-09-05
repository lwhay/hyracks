/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class SortGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final int SORT_GROUP_ACTIVITY_ID = 0;

    private final int[] sortFields;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final int framesLimit;

    public SortGroupOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IAggregatorDescriptor aggregator,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        if (framesLimit <= 1) {
            // minimum of 2 fames (1 in,1 out)
            throw new IllegalStateException();
        }
        recordDescriptors[0] = recordDescriptor;
        
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }
}
