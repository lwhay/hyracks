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
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper;

public class HashGroupSortMergeGrouper implements IPushBasedGrouper {

    private HashGrouper hashGrouper;

    private MergeGrouper mergeGrouper;

    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    protected final int[] decorFields;
    private final int framesLimit;
    private final int tableSize;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;

    private List<RunFileReader> runsFromHashGrouper;
    private RunFileWriter currentRunWriter;

    public HashGroupSortMergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            int tableSize, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.hashFunctionFactories = hashFunctionFactories;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;

        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;
    }

    @Override
    public void init() throws HyracksDataException {
        this.hashGrouper = new HashGrouper(ctx, framesLimit, tableSize, keyFields, decorFields, comparatorFactories,
                hashFunctionFactories, firstKeyNormalizerFactory, aggregatorFactory, finalMergerFactory, inRecordDesc,
                outRecordDesc, true, false);
        this.runsFromHashGrouper = new LinkedList<RunFileReader>();
        this.hashGrouper.init();
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer, int tupleIndexOffset) throws HyracksDataException {
        while (!hashGrouper.nextFrame(buffer, tupleIndexOffset)) {
            int processedTupleCount = hashGrouper.getProcessedTupleCount();
            tupleIndexOffset += processedTupleCount;
            currentRunWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(SortGroupMergeGrouper.class
                    .getSimpleName() + "_sort"), ctx.getIOManager());
            currentRunWriter.open();
            try {
                this.hashGrouper.flush(currentRunWriter, 0);
                this.hashGrouper.reset();
            } finally {
                currentRunWriter.close();
                runsFromHashGrouper.add(currentRunWriter.createReader());
            }
        }
        return true;
    }

    @Override
    public int[] getDataDistHistogram() throws HyracksDataException {
        return this.hashGrouper.getDataDistHistogram();
    }

    @Override
    public void flush(IFrameWriter writer, int flushOption) throws HyracksDataException {
        if (hashGrouper.getTuplesInHashTable() > 0) {
            if (runsFromHashGrouper.size() == 0) {
                // directly flush hash grouper into the writer
                this.hashGrouper.setSortOutput(false);
                this.hashGrouper.flush(writer, 1);
            } else {
                // create another run
                currentRunWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(SortGroupMergeGrouper.class
                        .getSimpleName() + "_sort"), ctx.getIOManager());
                currentRunWriter.open();
                try {
                    this.hashGrouper.flush(currentRunWriter, 0);
                    this.hashGrouper.reset();
                } finally {
                    currentRunWriter.close();
                    runsFromHashGrouper.add(currentRunWriter.createReader());
                }
            }
        }
        if (runsFromHashGrouper.size() > 0) {
            int[] mergeKeyFields = new int[keyFields.length];
            for (int i = 0; i < mergeKeyFields.length; i++) {
                mergeKeyFields[i] = i;
            }
            this.mergeGrouper = new MergeGrouper(ctx, mergeKeyFields, decorFields, framesLimit, tableSize,
                    comparatorFactories, hashFunctionFactories, partialMergerFactory, finalMergerFactory,
                    outRecordDesc, outRecordDesc);
            this.mergeGrouper.process(runsFromHashGrouper, writer);
        }
    }

    @Override
    public void reset() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws HyracksDataException {

    }

}
