/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractTaskState;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.SRHybridHashSortGroupHashTable;

public class SRHybridHashSortGroupOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final int AGGREGATE_ACTIVITY_ID = 0;

    private static final int MERGE_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private final int[] keyFields, storedKeyFields;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final IAggregatorDescriptorFactory mergerFactory;

    private final int framesLimit;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final int tableSize;

    private final boolean isLoadOptimized;

    private final static Logger LOGGER = Logger
            .getLogger(SRHybridHashSortGroupOperatorDescriptor.class.getSimpleName());

    public SRHybridHashSortGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor) {
        this(spec, keyFields, framesLimit, tableSize, comparatorFactories, hashFunctionFactories, null,
                aggregatorFactory, mergerFactory, recordDescriptor, false);
    }

    public SRHybridHashSortGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor, boolean isLoadOpt) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        if (framesLimit <= 2) {
            /**
             * Minimum of 2 frames: 1 for input records, and 1 for output
             * aggregation results.
             */
            throw new IllegalStateException("frame limit should at least be 3, but it is " + framesLimit + "!");
        }

        storedKeyFields = new int[keyFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.hashFunctionFactories = hashFunctionFactories;
        this.tableSize = tableSize;

        /**
         * Set the record descriptor. Note that since this operator is a unary
         * operator, only the first record descriptor is used here.
         */
        recordDescriptors[0] = recordDescriptor;

        this.isLoadOptimized = isLoadOpt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor#contributeActivities(edu.uci.ics.hyracks.api.dataflow.
     * IActivityGraphBuilder)
     */
    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        AggregateActivity aggregateAct = new AggregateActivity(new ActivityId(getOperatorId(), AGGREGATE_ACTIVITY_ID));
        MergeActivity mergeAct = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(aggregateAct);
        builder.addSourceEdge(0, aggregateAct, 0);

        builder.addActivity(mergeAct);
        builder.addTargetEdge(0, mergeAct, 0);

        builder.addBlockingEdge(aggregateAct, mergeAct);
    }

    public static class AggregateActivityState extends AbstractTaskState {

        private ISerializableGroupHashTable gTable;

        public AggregateActivityState() {
        }

        private AggregateActivityState(JobId jobId, TaskId tId) {
            super(jobId, tId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    private class AggregateActivity extends AbstractActivityNode {

        private static final long serialVersionUID = 1L;

        public AggregateActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryInputSinkOperatorNodePushable() {

                ISerializableGroupHashTable serializableGroupHashtable;

                FrameTupleAccessor accessor;

                // FIXME
                long timer;

                @Override
                public void open() throws HyracksDataException {

                    // FIXME
                    timer = System.currentTimeMillis();
                    LOGGER.warning("SRHybridHashSort-Agg-Open\t" + ctx.getIOManager().toString());

                    RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
                    ITuplePartitionComputerFactory aggregateTpcf = new FieldHashPartitionComputerFactory(keyFields,
                            hashFunctionFactories);
                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparatorFactories.length; i++) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }
                    serializableGroupHashtable = new SRHybridHashSortGroupHashTable(ctx, framesLimit, tableSize,
                            keyFields, comparators, aggregateTpcf.createPartitioner(),
                            firstNormalizerFactory == null ? null : firstNormalizerFactory
                                    .createNormalizedKeyComputer(), aggregatorFactory.createAggregator(ctx, inRecDesc,
                                    recordDescriptors[0], keyFields, storedKeyFields), inRecDesc, recordDescriptors[0]);
                    accessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        serializableGroupHashtable.insert(accessor, i);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    // TODO Auto-generated method stub

                }

                @Override
                public void close() throws HyracksDataException {
                    serializableGroupHashtable.finishup();
                    AggregateActivityState state = new AggregateActivityState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.gTable = serializableGroupHashtable;
                    ctx.setTaskState(state);

                    // FIXME
                    timer = System.currentTimeMillis() - timer;
                    LOGGER.warning("SRHybridHashSort-Agg-Close\t" + timer + "\t" + ctx.getIOManager().toString() + "\t"
                            + serializableGroupHashtable.getRunFileReaders().size());
                }
            };
        }
    }

    private class MergeActivity extends AbstractActivityNode {

        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {

            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                public void initialize() throws HyracksDataException {

                    // FIXME
                    long timer = System.currentTimeMillis();
                    LOGGER.warning(SRHybridHashSortGroupOperatorDescriptor.class.getSimpleName() + "-Merge-Open\t"
                            + ctx.getIOManager().toString());

                    AggregateActivityState aggState = (AggregateActivityState) ctx.getTaskState(new TaskId(
                            new ActivityId(getOperatorId(), AGGREGATE_ACTIVITY_ID), partition));

                    LinkedList<RunFileReader> runs = aggState.gTable.getRunFileReaders();

                    writer.open();
                    if (runs.size() <= 0) {
                        aggState.gTable.flushHashtableToOutput(writer);
                        aggState.gTable.close();
                    } else {
                        aggState.gTable.close();
                        ITuplePartitionComputerFactory mergeTpcf = new FieldHashPartitionComputerFactory(
                                storedKeyFields, hashFunctionFactories);
                        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                        for (int i = 0; i < comparatorFactories.length; i++) {
                            comparators[i] = comparatorFactories[i].createBinaryComparator();
                        }
                        //                        HybridHashSortGrouperBucketMerge merger = new HybridHashSortGrouperBucketMerge(ctx,
                        //                                storedKeyFields, framesLimit, tableSize, mergeTpcf.createPartitioner(), comparators,
                        //                                mergerFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                        //                                        storedKeyFields, storedKeyFields), recordDescriptors[0], recordDescriptors[0],
                        //                                writer, isLoadOptimized);
                        //
                        //                        merger.initialize(aggState.gTable.getRunFileReaders());

                        HybridHashSortRunMerger merger = new HybridHashSortRunMerger(ctx, runs, storedKeyFields,
                                comparators, recordDescriptors[0], mergeTpcf.createPartitioner(),
                                mergerFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                                        storedKeyFields, storedKeyFields), framesLimit, tableSize, writer,
                                isLoadOptimized);

                        merger.process();
                    }

                    writer.close();

                    // FIXME
                    timer = System.currentTimeMillis() - timer;
                    LOGGER.warning(SRHybridHashSortGroupOperatorDescriptor.class.getSimpleName() + "-Merge-Close\t"
                            + timer + "\t" + ctx.getIOManager().toString());
                }

            };
        }
    }

}
