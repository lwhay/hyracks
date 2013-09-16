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
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper;

public class SortGroupMergeGrouper implements IPushBasedGrouper {

    private SortGrouper sortGrouper;

    private MergeGrouper mergeGrouper;

    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    protected final int[] decorFields;
    private final int framesLimit;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;

    private List<RunFileReader> runsFromSortGrouper;
    private RunFileWriter currentRunWriter;

    public SortGroupMergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;

        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#init()
     */
    @Override
    public void init() throws HyracksDataException {
        this.sortGrouper = new SortGrouper(ctx, keyFields, decorFields, framesLimit, firstKeyNormalizerFactory,
                comparatorFactories, aggregatorFactory, inRecordDesc, outRecordDesc);
        this.runsFromSortGrouper = new LinkedList<RunFileReader>();
        sortGrouper.init();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!sortGrouper.nextFrame(buffer)) {
            currentRunWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(SortGroupMergeGrouper.class
                    .getSimpleName() + "_sort"), ctx.getIOManager());
            currentRunWriter.open();
            try {
                this.sortGrouper.flush(currentRunWriter);
                this.sortGrouper.reset();
            } finally {
                currentRunWriter.close();
                runsFromSortGrouper.add(currentRunWriter.createReader());
            }
            return sortGrouper.nextFrame(buffer);
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#getDataDistHistogram()
     */
    @Override
    public int[] getDataDistHistogram() throws HyracksDataException {
        return this.sortGrouper.getDataDistHistogram();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#flush(edu.uci.ics.hyracks.api.comm.IFrameWriter
     * )
     */
    @Override
    public void flush(IFrameWriter writer) throws HyracksDataException {
        if (sortGrouper.getFrameCount() > 0) {
            currentRunWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(SortGroupMergeGrouper.class
                    .getSimpleName() + "_sort"), ctx.getIOManager());
            currentRunWriter.open();
            this.sortGrouper.flush(currentRunWriter);
            currentRunWriter.close();
            runsFromSortGrouper.add(currentRunWriter.createReader());
            currentRunWriter = null;
            this.sortGrouper.close();
        }
        int[] mergeKeyFields = new int[keyFields.length];
        for (int i = 0; i < mergeKeyFields.length; i++) {
            mergeKeyFields[i] = i;
        }
        this.mergeGrouper = new MergeGrouper(ctx, mergeKeyFields, decorFields, framesLimit, comparatorFactories,
                partialMergerFactory, finalMergerFactory, outRecordDesc, outRecordDesc);
        this.mergeGrouper.process(runsFromSortGrouper, writer);

    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#reset()
     */
    @Override
    public void reset() throws HyracksDataException {
        // do nothing
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#close()
     */
    @Override
    public void close() throws HyracksDataException {
        // do nothing
    }

}
