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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.RepartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractTaskState;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.ISpillableTable;
import edu.uci.ics.hyracks.dataflow.std.group.ISpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class HybridHashGroupJoinOperatorDescriptor extends AbstractOperatorDescriptor {
	
	/* copied from HybridHashJoin */
	
    private static final int R_PARTITION_ACTIVITY_ID = 0;
    private static final int S_AND_JOIN_ACTIVITY_ID = 1;
    private static final int MERGE_ACTIVITY_ID = 2;

    private final int memsize;
    private static final long serialVersionUID = 1L;
    private final int inputsize0;
    private final double factor;
    private final int recordsPerFrame;
    private final int framesLimit;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] joinComparatorFactories;
    private final IBinaryComparatorFactory[] groupComparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final INullWriterFactory[] nullWriterFactories1;
    private final ISpillableTableFactory[] spillableTableFactories;
    private final ISpillableTableFactory spillableTableFactory0;
    private final ISpillableTableFactory spillableTableFactory1;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;
    private LinkedList<RunFileReader> runs;
    private final RecordDescriptor inRecordDescriptor;

    /**
     * @param spec
     * @param memsize
     *            in frames
     * @param inputsize0
     *            in frames
     * @param recordsPerFrame
     * @param factor
     * @param framesLimit TODO
     * @param keys0
     * @param keys1
     * @param hashFunctionFactories
     * @param groupComparatorFactories
     * @param firstNormalizerFactory TODO
     * @param inRecordDescriptor TODO
     * @param outRecordDescriptor
     * @param spillableTableFactory0 TODO
     * @param spillableTableFactory1 TODO
     * @throws HyracksDataException
     */
    
    public HybridHashGroupJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame, double factor,
    		int framesLimit, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
    		IBinaryComparatorFactory[] joinComparatorFactories, IBinaryComparatorFactory[] groupComparatorFactories,
    		INormalizedKeyComputerFactory firstNormalizerFactory, IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDescriptor,
    		RecordDescriptor outRecordDescriptor, ISpillableTableFactory spillableTableFactory0, ISpillableTableFactory spillableTableFactory1,
    		INullWriterFactory[] nullWriterFactories1) throws HyracksDataException {
        super(spec, 2, 1);
        
/*        if (framesLimit < 2) {
            /**
             * Minimum of 2 frames: 1 for input records, and 1 for output
             * aggregation results.
             */
//            throw new IllegalStateException("frame limit should at least be 2, but it is " + framesLimit + "!");
//        }

        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.factor = factor;
        this.recordsPerFrame = recordsPerFrame;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.framesLimit = framesLimit;
        this.aggregatorFactory = aggregatorFactory;
        this.hashFunctionFactories = hashFunctionFactories;
        this.groupComparatorFactories = groupComparatorFactories;
        this.joinComparatorFactories = joinComparatorFactories;
        this.spillableTableFactories = new ISpillableTableFactory[2];
        this.spillableTableFactories[0] = spillableTableFactory0;
        this.spillableTableFactories[1] = spillableTableFactory1;
        this.spillableTableFactory0 = spillableTableFactory0;
        this.spillableTableFactory1 = spillableTableFactory1;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.nullWriterFactories1 = nullWriterFactories1;
        recordDescriptors[0] = outRecordDescriptor;
        this.inRecordDescriptor = inRecordDescriptor;
        runs = new LinkedList<RunFileReader>();
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        PartitionActivity phase1 = new PartitionActivity(new ActivityId(odId,
                R_PARTITION_ACTIVITY_ID), keys0, 0);
        PartitionActivity phase2 = new PartitionActivity(new ActivityId(odId,
                S_AND_JOIN_ACTIVITY_ID), keys1, 1);
        MergeActivity merge = new MergeActivity(new ActivityId(odId,
                MERGE_ACTIVITY_ID));

        builder.addActivity(phase1);
        builder.addSourceEdge(0, phase1, 0);

        builder.addActivity(phase2);
        builder.addSourceEdge(1, phase2, 0);

        builder.addActivity(merge);
        
        builder.addBlockingEdge(phase1, phase2);
        builder.addBlockingEdge(phase2, merge);

        builder.addTargetEdge(0, merge, 0);
    }

    public static class PartitionTaskState extends AbstractTaskState {
        private RunFileWriter[] fWriters;
        private InMemoryHashGroupJoin joiner;
        private int nPartitions;
        private int memoryForHashtable;

        private LinkedList<RunFileReader> runs;

        private ISpillableTable gTable;

        public PartitionTaskState() {
        }

        private PartitionTaskState(JobId jobId, TaskId tId) {
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

    private class PartitionActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private final int[] pKeys;
        private final int inputIndex;

        public PartitionActivity(ActivityId id, int[] partitionKeys, int inputIndex) {
            super(id);
            this.pKeys = partitionKeys;
            this.inputIndex = inputIndex;
            
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), inputIndex);
            final IBinaryComparator[] comparators = new IBinaryComparator[groupComparatorFactories.length];
            for (int i = 0; i < groupComparatorFactories.length; ++i) {
                comparators[i] = groupComparatorFactories[i].createBinaryComparator();
            }
            final INullWriter[] nullWriters1 = new INullWriter[nullWriterFactories1.length];
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
            }

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private PartitionTaskState state = new PartitionTaskState(ctx.getJobletContext()
                        .getJobId(), new TaskId(getActivityId(), partition));
                private final FrameTupleAccessor accessorBuild = new FrameTupleAccessor(ctx.getFrameSize(), rd0);
                
                @Override
                public void open() throws HyracksDataException {
                	System.out.println("*** Start partitioning " + inputIndex);
                	
                    state.runs = HybridHashGroupJoinOperatorDescriptor.this.runs;
                    state.gTable = spillableTableFactories[inputIndex].buildSpillableTable(ctx, pKeys, groupComparatorFactories,
                            firstNormalizerFactory, aggregatorFactory, rd0, recordDescriptors[0], framesLimit);
                    state.gTable.reset();

/*                    if (memsize > 1) {
                        if (memsize > inputsize0) {
                            state.nPartitions = 0;
                        } else {
                            state.nPartitions = (int) (Math.ceil((double) (inputsize0 * factor / nPartitions - memsize)
                                    / (double) (memsize - 1)));
                            System.out.println("** state.nPartitions = " + state.nPartitions);
                        }
                        if (state.nPartitions <= 0) {
                            // becomes in-memory HJ
                            state.memoryForHashtable = memsize - 2;
                            state.nPartitions = 0;
                        } else {
                            state.memoryForHashtable = memsize - state.nPartitions - 2;
                            if (state.memoryForHashtable < 0) {
                                state.memoryForHashtable = 0;
                                state.nPartitions = (int) Math.ceil(Math.sqrt(inputsize0 * factor / nPartitions));
                            }
                        }
                    } else {
                        throw new HyracksDataException("not enough memory");
                    }

//                    ITuplePartitionComputer hpc0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)
//                            .createPartitioner();
//                    ITuplePartitionComputer hpc1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)
//                            .createPartitioner();
//                    ITuplePartitionComputerFactory hpcf0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories);
                    ITuplePartitionComputerFactory hpcf1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories);
                    int tableSize = (int) (state.memoryForHashtable * recordsPerFrame * factor);
                    
                    
                    state.joiner = new InMemoryHashGroupJoin(ctx, tableSize, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                            new FrameTupleAccessor(ctx.getFrameSize(), rd1), groupComparatorFactories, hpcf0, hpcf1,
                            rd0, recordDescriptors[0], aggregatorFactory, keys1, keys0, nullWriters1);
                    bufferForPartitions = new ByteBuffer[state.nPartitions];
                    state.fWriters = new RunFileWriter[state.nPartitions];
                    for (int i = 0; i < state.nPartitions; i++) {
                        bufferForPartitions[i] = ctx.allocateFrame();
                    }

                    ftappender.reset(inBuffer, true);
**/                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessorBuild.reset(buffer);
                    int tupleCount = accessorBuild.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        /**
                         * If the group table is too large, flush the table into
                         * a run file.
                         */
                        if (!state.gTable.insert(accessorBuild, i)) {
                            flushFramesToRun();
                            if (!state.gTable.insert(accessorBuild, i))
                                throw new HyracksDataException(
                                        "Failed to insert a new buffer into the aggregate operator!");
                        }
                    }

/*                    if (state.memoryForHashtable != memsize - 2) {
                        accessorBuild.reset(buffer);
                        int tCount = accessorBuild.getTupleCount();
                        for (int i = 0; i < tCount; ++i) {
                            int entry = -1;
                            if (state.memoryForHashtable == 0) {
                                entry = hpcBuild.partition(accessorBuild, i, state.nPartitions);
                                boolean newBuffer = false;
                                ByteBuffer bufBi = bufferForPartitions[entry];
                                while (true) {
                                    appender.reset(bufBi, newBuffer);
                                    if (appender.append(accessorBuild, i)) {
                                        break;
                                    } else {
                                        write(entry, bufBi);
                                        bufBi.clear();
                                        newBuffer = true;
                                    }
                                }
                            } else {
                                entry = hpcBuild.partition(accessorBuild, i, (int) (inputsize0 * factor / nPartitions));
                                if (entry < state.memoryForHashtable) {
                                    while (true) {
                                        if (!ftappender.append(accessorBuild, i)) {
                                            build(inBuffer);

                                            ftappender.reset(inBuffer, true);
                                        } else {
                                            break;
                                        }
                                    }
                                } else {
                                    entry %= state.nPartitions;
                                    boolean newBuffer = false;
                                    ByteBuffer bufBi = bufferForPartitions[entry];
                                    while (true) {
                                        appender.reset(bufBi, newBuffer);
                                        if (appender.append(accessorBuild, i)) {
                                            break;
                                        } else {
                                            write(entry, bufBi);
                                            bufBi.clear();
                                            newBuffer = true;
                                        }
                                    }
                                }
                            }

                        }
                    } else {
                        build(buffer);
                    }

*/                }

                private void build(ByteBuffer inBuffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame();
                    FrameUtils.copy(inBuffer, copyBuffer);
//                    state.joiner.build(copyBuffer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    throw new HyracksDataException("HybridHashGroup failed in partition phase " + PartitionActivity.this.inputIndex);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state.gTable.getFrameCount() >= 0) {
//                        if (state.runs.size() > 0) {
                            /**
                             * flush the memory into the run file.
                             */
                            flushFramesToRun();
                            state.gTable.close();
                            state.gTable = null;
//                        }
                    }
                    System.out.println("*** runs size = " + runs.size());
                    ctx.setTaskState(state);
                    
/*                    if (state.memoryForHashtable != 0)
                        build(inBuffer);

                    for (int i = 0; i < state.nPartitions; i++) {
                        ByteBuffer buf = bufferForPartitions[i];
                        accessorBuild.reset(buf);
                        if (accessorBuild.getTupleCount() > 0) {
                            write(i, buf);
                        }
                        closeWriter(i);
                    }

                    ctx.setTaskState(state);
*/                }

 /*               private void closeWriter(int i) throws HyracksDataException {
                    RunFileWriter writer = state.fWriters[i];
                    if (writer != null) {
                        writer.close();
                    }
                }

                private void write(int i, ByteBuffer head) throws HyracksDataException {
                    RunFileWriter writer = state.fWriters[i];
                    if (writer == null) {
                        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                                BuildAndPartitionActivityNode.class.getSimpleName());
                        writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        state.fWriters[i] = writer;
                    }
                    writer.nextFrame(head);
                }
 */               
                private void flushFramesToRun() throws HyracksDataException {
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                HybridHashGroupJoinOperatorDescriptor.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    RunFileWriter writer = new RunFileWriter(runFile, ctx.getIOManager());
                    writer.open();
                    try {
                        state.gTable.sortFrames();
                        state.gTable.flushFrames(writer, true);
                    } catch (Exception ex) {
                        throw new HyracksDataException(ex);
                    } finally {
                        writer.close();
                    }
                    state.gTable.reset();
                    state.runs.add(((RunFileWriter) writer).createReader());
                    System.out.println("\t*** Flushed. runs size = " + state.runs.size());
                }

            };
            return op;
        }
    }
    
    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            final IBinaryComparator[] comparators = new IBinaryComparator[groupComparatorFactories.length];
            for (int i = 0; i < groupComparatorFactories.length; ++i) {
                comparators[i] = groupComparatorFactories[i].createBinaryComparator();
            }

            int[] keyFieldsInPartialResults = new int[keys0.length];
            for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
                keyFieldsInPartialResults[i] = i;
            }

            final int[] storedKeys = new int[keys0.length];
            /**
             * Get the list of the fields in the stored records.
             */
            @SuppressWarnings("rawtypes")
            ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keys0.length + 1];
            for (int i = 0; i < keys0.length; ++i) {
                storedKeys[i] = i;
                storedKeySerDeser[i] = inRecordDescriptor.getFields()[keys0[i]];
            }

            RecordDescriptor storedKeysDescriptor = new RecordDescriptor(storedKeySerDeser);

            final IAggregatorDescriptor aggregator = aggregatorFactory.createAggregator(ctx, storedKeysDescriptor,
                    recordDescriptors[0], keys0, keyFieldsInPartialResults);
            final AggregateState aggregateState = aggregator.createAggregateStates();

            final ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(recordDescriptors[0].getFields().length);

            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                /**
                 * Input frames, one for each run file.
                 */
                private List<ByteBuffer> inFrames;

                /**
                 * Output frame.
                 */
                private ByteBuffer outFrame, writerFrame;
                private final FrameTupleAppender outAppender = new FrameTupleAppender(ctx.getFrameSize());
                private FrameTupleAppender writerAppender;

                private LinkedList<RunFileReader> runs;

//                private AggregateActivityState aggState;

                private ArrayTupleBuilder finalTupleBuilder;

                /**
                 * how many frames to be read ahead once
                 */
                private int runFrameLimit = 1;

                private int[] currentFrameIndexInRun;
                private int[] currentRunFrames;

                private final FrameTupleAccessor outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(),
                        recordDescriptors[0]);

                public void initialize() throws HyracksDataException {
//                    aggState = (AggregateActivityState) ctx.getTaskState(new TaskId(new ActivityId(getOperatorId(),
//                            AGGREGATE_ACTIVITY_ID), partition));
                    runs = HybridHashGroupJoinOperatorDescriptor.this.runs;
                    writer.open();
                    try {
/*                        if (runs.size() <= 0) {
                            ISpillableTable gTable = aggState.gTable;
                            if (gTable != null) {
                                if (isOutputSorted)
                                    gTable.sortFrames();
                                gTable.flushFrames(writer, false);
                            }
                            gTable = null;
                            aggState = null;
                            System.gc();
                        } else {
                            aggState = null;
*/                            System.gc();
//                            runs = new LinkedList<RunFileReader>(runs);
                            runs = HybridHashGroupJoinOperatorDescriptor.this.runs;
                            System.out.println("*** runs size = " + runs.size());
                            inFrames = new ArrayList<ByteBuffer>();
                            outFrame = ctx.allocateFrame();
                            outAppender.reset(outFrame, true);
                            outFrameAccessor.reset(outFrame);
                            while (runs.size() > 0) {
                                try {
                                    doPass(runs);
                                } catch (Exception e) {
                                    throw new HyracksDataException(e);
                                }
                            }
                            inFrames.clear();
//                        }
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    } finally {
                        aggregateState.close();
                        writer.close();
                    }
                }

                private void doPass(LinkedList<RunFileReader> runs) throws HyracksDataException {
                    FileReference newRun = null;
                    IFrameWriter writer = this.writer;
                    boolean finalPass = false;

                    while (inFrames.size() + 2 < framesLimit) {
                        inFrames.add(ctx.allocateFrame());
                    }
                    int runNumber;
                    if (runs.size() + 2 <= framesLimit) {
                        finalPass = true;
                        runFrameLimit = (framesLimit - 2) / runs.size();
                        runNumber = runs.size();
                    } else {
                        runNumber = framesLimit - 2;
                        newRun = ctx.getJobletContext().createManagedWorkspaceFile(
                                HybridHashGroupJoinOperatorDescriptor.class.getSimpleName());
                        writer = new RunFileWriter(newRun, ctx.getIOManager());
                        writer.open();
                    }
                    try {
                        currentFrameIndexInRun = new int[runNumber];
                        currentRunFrames = new int[runNumber];
                        /**
                         * Create file readers for each input run file, only for
                         * the ones fit into the inFrames
                         */
                        RunFileReader[] runFileReaders = new RunFileReader[runNumber];
                        FrameTupleAccessor[] tupleAccessors = new FrameTupleAccessor[inFrames.size()];
                        Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
                        ReferencedPriorityQueue topTuples = new ReferencedPriorityQueue(ctx.getFrameSize(),
                                recordDescriptors[0], runNumber, comparator);
                        /**
                         * current tuple index in each run
                         */
                        int[] tupleIndices = new int[runNumber];

                        for (int runIndex = runNumber - 1; runIndex >= 0; runIndex--) {
                            tupleIndices[runIndex] = 0;
                            // Load the run file
                            runFileReaders[runIndex] = runs.get(runIndex);
                            runFileReaders[runIndex].open();

                            currentRunFrames[runIndex] = 0;
                            currentFrameIndexInRun[runIndex] = runIndex * runFrameLimit;
                            for (int j = 0; j < runFrameLimit; j++) {
                                int frameIndex = currentFrameIndexInRun[runIndex] + j;
                                if (runFileReaders[runIndex].nextFrame(inFrames.get(frameIndex))) {
                                    tupleAccessors[frameIndex] = new FrameTupleAccessor(ctx.getFrameSize(),
                                            recordDescriptors[0]);
                                    tupleAccessors[frameIndex].reset(inFrames.get(frameIndex));
                                    currentRunFrames[runIndex]++;
                                    if (j == 0)
                                        setNextTopTuple(runIndex, tupleIndices, runFileReaders, tupleAccessors,
                                                topTuples);
                                } else {
                                    break;
                                }
                            }
                        }

                        /**
                         * Start merging
                         */
                        while (!topTuples.areRunsExhausted()) {
                            /**
                             * Get the top record
                             */
                            ReferenceEntry top = topTuples.peek();
                            int tupleIndex = top.getTupleIndex();
                            int runIndex = topTuples.peek().getRunid();
                            FrameTupleAccessor fta = top.getAccessor();

                            int currentTupleInOutFrame = outFrameAccessor.getTupleCount() - 1;
                            if (currentTupleInOutFrame < 0
                                    || compareFrameTuples(fta, tupleIndex, outFrameAccessor, currentTupleInOutFrame) != 0) {
                                /**
                                 * Initialize the first output record Reset the
                                 * tuple builder
                                 */

                                tupleBuilder.reset();
                                
                                for(int k = 0; k < storedKeys.length; k++){
                                	tupleBuilder.addField(fta, tupleIndex, storedKeys[k]);
                                }

                                aggregator.init(tupleBuilder, fta, tupleIndex, aggregateState);

                                if (!outAppender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(),
                                        tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                                    flushOutFrame(writer, finalPass);
                                    if (!outAppender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(),
                                            tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                                        throw new HyracksDataException(
                                                "The partial result is too large to be initialized in a frame.");
                                    }
                                }

                            } else {
                                /**
                                 * if new tuple is in the same group of the
                                 * current aggregator do merge and output to the
                                 * outFrame
                                 */

                                aggregator.aggregate(fta, tupleIndex, outFrameAccessor, currentTupleInOutFrame,
                                        aggregateState);

                            }
                            tupleIndices[runIndex]++;
                            setNextTopTuple(runIndex, tupleIndices, runFileReaders, tupleAccessors, topTuples);
                        }

                        if (outAppender.getTupleCount() > 0) {
                            flushOutFrame(writer, finalPass);
                            outAppender.reset(outFrame, true);
                        }

                        aggregator.close();

                        runs.subList(0, runNumber).clear();
                        /**
                         * insert the new run file into the beginning of the run
                         * file list
                         */
                        if (!finalPass) {
                            runs.add(0, ((RunFileWriter) writer).createReader());
                        }
                    } finally {
                        if (!finalPass) {
                            writer.close();
                        }
                    }
                }

                private void flushOutFrame(IFrameWriter writer, boolean isFinal) throws HyracksDataException {

                    if (finalTupleBuilder == null) {
                        finalTupleBuilder = new ArrayTupleBuilder(recordDescriptors[0].getFields().length);
                    }

                    if (writerFrame == null) {
                        writerFrame = ctx.allocateFrame();
                    }

                    if (writerAppender == null) {
                        writerAppender = new FrameTupleAppender(ctx.getFrameSize());
                        writerAppender.reset(writerFrame, true);
                    }

                    outFrameAccessor.reset(outFrame);

                    for (int i = 0; i < outFrameAccessor.getTupleCount(); i++) {

                        finalTupleBuilder.reset();

                        for (int k = 0; k < storedKeys.length; k++) {
                            finalTupleBuilder.addField(outFrameAccessor, i, storedKeys[k]);
                        }

                        if (isFinal) {

                            aggregator.outputFinalResult(finalTupleBuilder, outFrameAccessor, i, aggregateState);

                        } else {

                            aggregator.outputPartialResult(finalTupleBuilder, outFrameAccessor, i, aggregateState);
                        }

                        if (!writerAppender.appendSkipEmptyField(finalTupleBuilder.getFieldEndOffsets(),
                                finalTupleBuilder.getByteArray(), 0, finalTupleBuilder.getSize())) {
                            FrameUtils.flushFrame(writerFrame, writer);
                            writerAppender.reset(writerFrame, true);
                            if (!writerAppender.appendSkipEmptyField(finalTupleBuilder.getFieldEndOffsets(),
                                    finalTupleBuilder.getByteArray(), 0, finalTupleBuilder.getSize())) {
                                throw new HyracksDataException(
                                        "Aggregation output is too large to be fit into a frame.");
                            }
                        }
                    }
                    if (writerAppender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(writerFrame, writer);
                        writerAppender.reset(writerFrame, true);
                    }

                    outAppender.reset(outFrame, true);
                }

                private void setNextTopTuple(int runIndex, int[] tupleIndices, RunFileReader[] runCursors,
                        FrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples)
                        throws HyracksDataException {
                    int runStart = runIndex * runFrameLimit;
                    boolean existNext = false;
                    if (tupleAccessors[currentFrameIndexInRun[runIndex]] == null || runCursors[runIndex] == null) {
                        /**
                         * run already closed
                         */
                        existNext = false;
                    } else if (currentFrameIndexInRun[runIndex] - runStart < currentRunFrames[runIndex] - 1) {
                        /**
                         * not the last frame for this run
                         */
                        existNext = true;
                        if (tupleIndices[runIndex] >= tupleAccessors[currentFrameIndexInRun[runIndex]].getTupleCount()) {
                            tupleIndices[runIndex] = 0;
                            currentFrameIndexInRun[runIndex]++;
                        }
                    } else if (tupleIndices[runIndex] < tupleAccessors[currentFrameIndexInRun[runIndex]]
                            .getTupleCount()) {
                        /**
                         * the last frame has expired
                         */
                        existNext = true;
                    } else {
                        /**
                         * If all tuples in the targeting frame have been
                         * checked.
                         */
                        tupleIndices[runIndex] = 0;
                        currentFrameIndexInRun[runIndex] = runStart;
                        /**
                         * read in batch
                         */
                        currentRunFrames[runIndex] = 0;
                        for (int j = 0; j < runFrameLimit; j++) {
                            int frameIndex = currentFrameIndexInRun[runIndex]
                                    + j;
                            if (runCursors[runIndex].nextFrame(inFrames
                                    .get(frameIndex))) {
                                tupleAccessors[frameIndex].reset(inFrames
                                        .get(frameIndex));
                                existNext = true;
                                currentRunFrames[runIndex]++;
                            } else {
                                break;
                            }
                        }
                    }

                    if (existNext) {
                        topTuples.popAndReplace(tupleAccessors[currentFrameIndexInRun[runIndex]],
                                tupleIndices[runIndex]);
                    } else {
                        topTuples.pop();
                        closeRun(runIndex, runCursors, tupleAccessors);
                    }
                }

                /**
                 * Close the run file, and also the corresponding readers and
                 * input frame.
                 * 
                 * @param index
                 * @param runCursors
                 * @param tupleAccessor
                 * @throws HyracksDataException
                 */
                private void closeRun(int index, RunFileReader[] runCursors, IFrameTupleAccessor[] tupleAccessor)
                        throws HyracksDataException {
                    if (runCursors[index] != null) {
                        runCursors[index].close();
                        runCursors[index] = null;
                        int frameOffset = index * runFrameLimit;
                        for (int j = 0; j < runFrameLimit; j++) {
                            tupleAccessor[frameOffset + j] = null;
                        }
                    }
                }

                private int compareFrameTuples(IFrameTupleAccessor fta1, int j1, IFrameTupleAccessor fta2, int j2) {
                    byte[] b1 = fta1.getBuffer().array();
                    byte[] b2 = fta2.getBuffer().array();
                    for (int f = 0; f < keys0.length; ++f) {
                        int fIdx = f;
                        int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength()
                                + fta1.getFieldStartOffset(j1, fIdx);
                        int l1 = fta1.getFieldLength(j1, fIdx);
                        int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength()
                                + fta2.getFieldStartOffset(j2, fIdx);
                        int l2_start = fta2.getFieldStartOffset(j2, fIdx);
                        int l2_end = fta2.getFieldEndOffset(j2, fIdx);
                        int l2 = l2_end - l2_start;
                        int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                        if (c != 0) {
                            return c;
                        }
                    }
                    return 0;
                }
            };
            return op;
        }

        private Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {
            return new Comparator<ReferenceEntry>() {

                @Override
                public int compare(ReferenceEntry o1, ReferenceEntry o2) {
                    FrameTupleAccessor fta1 = (FrameTupleAccessor) o1.getAccessor();
                    FrameTupleAccessor fta2 = (FrameTupleAccessor) o2.getAccessor();
                    int j1 = o1.getTupleIndex();
                    int j2 = o2.getTupleIndex();
                    byte[] b1 = fta1.getBuffer().array();
                    byte[] b2 = fta2.getBuffer().array();
                    for (int f = 0; f < keys0.length; ++f) {
                        int fIdx = f;
                        int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength()
                                + fta1.getFieldStartOffset(j1, fIdx);
                        int l1 = fta1.getFieldEndOffset(j1, fIdx) - fta1.getFieldStartOffset(j1, fIdx);
                        int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength()
                                + fta2.getFieldStartOffset(j2, fIdx);
                        int l2 = fta2.getFieldEndOffset(j2, fIdx) - fta2.getFieldStartOffset(j2, fIdx);
                        int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                        if (c != 0) {
                            return c;
                        }
                    }
                    return 0;
                }

            };
        }

    }
}