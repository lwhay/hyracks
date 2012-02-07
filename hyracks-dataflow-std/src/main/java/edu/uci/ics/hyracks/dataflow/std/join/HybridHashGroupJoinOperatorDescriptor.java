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
import java.util.LinkedList;
import java.util.List;

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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.ISpillableTable;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor.AggregateActivityState;

public class HybridHashGroupJoinOperatorDescriptor extends AbstractOperatorDescriptor {
	
	/* copied from HybridHashJoin */
	
    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private final int memsize;
    private static final long serialVersionUID = 1L;
    private final int inputsize0;
    private final double factor;
    private final int recordsPerFrame;
//    private final int framesLimit;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] joinComparatorFactories;
    private final IBinaryComparatorFactory[] groupComparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final INullWriterFactory[] nullWriterFactories1;
//    private final ISpillableTableFactory spillableTableFactory;
//    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    /**
     * @param spec
     * @param memsize
     *            in frames
     * @param inputsize0
     *            in frames
     * @param recordsPerFrame
     * @param factor
     * @param keys0
     * @param keys1
     * @param hashFunctionFactories
     * @param groupComparatorFactories
     * @param recordDescriptor
     * @throws HyracksDataException
     */
    
    public HybridHashGroupJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame,
    		double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryComparatorFactory[] joinComparatorFactories,
    		IBinaryComparatorFactory[] groupComparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
    		RecordDescriptor recordDescriptor, INullWriterFactory[] nullWriterFactories1)
            throws HyracksDataException {
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
//        this.framesLimit = framesLimit;
        this.aggregatorFactory = aggregatorFactory;
        this.hashFunctionFactories = hashFunctionFactories;
        this.groupComparatorFactories = groupComparatorFactories;
        this.joinComparatorFactories = joinComparatorFactories;
//        this.spillableTableFactory = spillableTableFactory;
//        this.firstNormalizerFactory = firstNormalizerFactory;
        this.nullWriterFactories1 = nullWriterFactories1;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        BuildAndPartitionActivityNode phase1 = new BuildAndPartitionActivityNode(new ActivityId(odId,
                BUILD_AND_PARTITION_ACTIVITY_ID));
        PartitionAndJoinActivityNode phase2 = new PartitionAndJoinActivityNode(new ActivityId(odId,
                PARTITION_AND_JOIN_ACTIVITY_ID));

        builder.addActivity(phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(phase2);
        builder.addSourceEdge(0, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);
    }

    public static class BuildAndPartitionTaskState extends AbstractTaskState {
        private RunFileWriter[] fWriters;
        private InMemoryHashGroupJoin joiner;
        private int nPartitions;
        private int memoryForHashtable;

        private LinkedList<RunFileReader> runs;

        private ISpillableTable gTable;

        public BuildAndPartitionTaskState() {
        }

        private BuildAndPartitionTaskState(JobId jobId, TaskId tId) {
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

    private class BuildAndPartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public BuildAndPartitionActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            final IBinaryComparator[] comparators = new IBinaryComparator[groupComparatorFactories.length];
            for (int i = 0; i < groupComparatorFactories.length; ++i) {
                comparators[i] = groupComparatorFactories[i].createBinaryComparator();
            }
            final INullWriter[] nullWriters1 = new INullWriter[nullWriterFactories1.length];
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
            }

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(ctx.getJobletContext()
                        .getJobId(), new TaskId(getActivityId(), partition));
                private final FrameTupleAccessor accessorBuild = new FrameTupleAccessor(ctx.getFrameSize(), rd0);
                private final ITuplePartitionComputer hpcBuild = new FieldHashPartitionComputerFactory(keys0,
                        hashFunctionFactories).createPartitioner();
                private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                private final FrameTupleAppender ftappender = new FrameTupleAppender(ctx.getFrameSize());
                private ByteBuffer[] bufferForPartitions;
                private final ByteBuffer inBuffer = ctx.allocateFrame();

                @Override
                public void close() throws HyracksDataException {
                    if (state.memoryForHashtable != 0)
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
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

                    if (state.memoryForHashtable != memsize - 2) {
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

                }

                private void build(ByteBuffer inBuffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame();
                    FrameUtils.copy(inBuffer, copyBuffer);
                    state.joiner.build(copyBuffer);
                }

                @Override
                public void open() throws HyracksDataException {
                    if (memsize > 1) {
                        if (memsize > inputsize0) {
                            state.nPartitions = 0;
                        } else {
                            state.nPartitions = (int) (Math.ceil((double) (inputsize0 * factor / nPartitions - memsize)
                                    / (double) (memsize - 1)));
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

/*                    ITuplePartitionComputer hpc0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)
                            .createPartitioner();
                    ITuplePartitionComputer hpc1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)
                            .createPartitioner();
*/                    ITuplePartitionComputerFactory hpcf0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories);
                    ITuplePartitionComputerFactory hpcf1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories);
                    int tableSize = (int) (state.memoryForHashtable * recordsPerFrame * factor);
                    
                    
                    state.joiner = new InMemoryHashGroupJoin(ctx, tableSize, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                            new FrameTupleAccessor(ctx.getFrameSize(), rd1), groupComparatorFactories, hpcf0 /*gByTpc0*/, hpcf1 /*gByTpc1*/,
                            rd0, recordDescriptors[0], aggregatorFactory, keys1, keys0, nullWriters1);
                    bufferForPartitions = new ByteBuffer[state.nPartitions];
                    state.fWriters = new RunFileWriter[state.nPartitions];
                    for (int i = 0; i < state.nPartitions; i++) {
                        bufferForPartitions[i] = ctx.allocateFrame();
                    }

                    ftappender.reset(inBuffer, true);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                private void closeWriter(int i) throws HyracksDataException {
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
            };
            return op;
        }
    }
    
    private class PartitionAndJoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public PartitionAndJoinActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            final IBinaryComparator[] comparators = new IBinaryComparator[groupComparatorFactories.length];
            for (int i = 0; i < groupComparatorFactories.length; ++i) {
                comparators[i] = groupComparatorFactories[i].createBinaryComparator();
            }
            final INullWriter[] nullWriters1 = new INullWriter[nullWriterFactories1.length];
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
            }

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private BuildAndPartitionTaskState state;
                private final FrameTupleAccessor accessorProbe = new FrameTupleAccessor(ctx.getFrameSize(), rd1);
                private final ITuplePartitionComputerFactory hpcf0 = new FieldHashPartitionComputerFactory(keys0,
                        hashFunctionFactories);
                private final ITuplePartitionComputerFactory hpcf1 = new FieldHashPartitionComputerFactory(keys1,
                        hashFunctionFactories);
                private final ITuplePartitionComputer hpcProbe = hpcf0.createPartitioner();

                private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                private final FrameTupleAppender ftap = new FrameTupleAppender(ctx.getFrameSize());
                private final ByteBuffer inBuffer = ctx.allocateFrame();
                private final ByteBuffer outBuffer = ctx.allocateFrame();
                private RunFileWriter[] buildWriters;
                private RunFileWriter[] probeWriters;
                private ByteBuffer[] bufferForPartitions;

                @Override
                public void open() throws HyracksDataException {
                    state = (BuildAndPartitionTaskState) ctx.getTaskState(new TaskId(new ActivityId(getOperatorId(),
                            BUILD_AND_PARTITION_ACTIVITY_ID), partition));
                    writer.open();
                    buildWriters = state.fWriters;
                    probeWriters = new RunFileWriter[state.nPartitions];
                    bufferForPartitions = new ByteBuffer[state.nPartitions];
                    for (int i = 0; i < state.nPartitions; i++) {
                        bufferForPartitions[i] = ctx.allocateFrame();
                    }
                    appender.reset(outBuffer, true);
                    ftap.reset(inBuffer, true);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (state.memoryForHashtable != memsize - 2) {
                        accessorProbe.reset(buffer);
                        int tupleCount0 = accessorProbe.getTupleCount();
                        for (int i = 0; i < tupleCount0; ++i) {

                            int entry = -1;
                            if (state.memoryForHashtable == 0) {
                                entry = hpcProbe.partition(accessorProbe, i, state.nPartitions);
                                boolean newBuffer = false;
                                ByteBuffer outbuf = bufferForPartitions[entry];
                                while (true) {
                                    appender.reset(outbuf, newBuffer);
                                    if (appender.append(accessorProbe, i)) {
                                        break;
                                    } else {
                                        write(entry, outbuf);
                                        outbuf.clear();
                                        newBuffer = true;
                                    }
                                }
                            } else {
                                entry = hpcProbe.partition(accessorProbe, i, (int) (inputsize0 * factor / nPartitions));
                                if (entry < state.memoryForHashtable) {
                                    while (true) {
                                        if (!ftap.append(accessorProbe, i)) {
                                            state.joiner.join(inBuffer, writer);
                                            ftap.reset(inBuffer, true);
                                        } else
                                            break;
                                    }

                                } else {
                                    entry %= state.nPartitions;
                                    boolean newBuffer = false;
                                    ByteBuffer outbuf = bufferForPartitions[entry];
                                    while (true) {
                                        appender.reset(outbuf, newBuffer);
                                        if (appender.append(accessorProbe, i)) {
                                            break;
                                        } else {
                                            write(entry, outbuf);
                                            outbuf.clear();
                                            newBuffer = true;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        state.joiner.join(buffer, writer);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                	System.out.println("***\tClose1");
                    state.joiner.join(inBuffer, writer);
                    state.joiner.closeJoin(writer);
                    ITuplePartitionComputerFactory hpcRep0 = new RepartitionComputerFactory(state.nPartitions, hpcf0);
                    ITuplePartitionComputerFactory hpcRep1 = new RepartitionComputerFactory(state.nPartitions, hpcf1);
                    if (state.memoryForHashtable != memsize - 2) {
                    	System.out.println("***\tClose2");
                        for (int i = 0; i < state.nPartitions; i++) {
                            ByteBuffer buf = bufferForPartitions[i];
                            accessorProbe.reset(buf);
                            if (accessorProbe.getTupleCount() > 0) {
                                write(i, buf);
                            }
                            closeWriter(i);
                        }

                    	System.out.println("***\tClose3");
                        inBuffer.clear();
                        int tableSize = -1;
                        if (state.memoryForHashtable == 0) {
                            tableSize = (int) (state.nPartitions * recordsPerFrame * factor);
                        } else {
                            tableSize = (int) (memsize * recordsPerFrame * factor);
                        }
                    	System.out.println("***\tClose4");
                        for (int partitionid = 0; partitionid < state.nPartitions; partitionid++) {
                            RunFileWriter buildWriter = buildWriters[partitionid];
                            RunFileWriter probeWriter = probeWriters[partitionid];
                            if (buildWriter == null) {
                                continue;
                            }

                            state.joiner = new InMemoryHashGroupJoin(ctx, tableSize, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                                    new FrameTupleAccessor(ctx.getFrameSize(), rd1), groupComparatorFactories, hpcRep0 /*gByTpc0*/, hpcRep1 /*gByTpc1*/,
                                    rd0, recordDescriptors[0], aggregatorFactory, keys1, keys0, nullWriters1);

                            if (buildWriter != null) {
                                RunFileReader buildReader = buildWriter.createReader();
                                buildReader.open();
                                while (buildReader.nextFrame(inBuffer)) {
                                    ByteBuffer copyBuffer = ctx.allocateFrame();
                                    FrameUtils.copy(inBuffer, copyBuffer);
                                    state.joiner.build(copyBuffer);
                                    inBuffer.clear();
                                }
                                buildReader.close();
                            }

                        	System.out.println("***\tClose5");
                            // probe
                            if (probeWriter != null) {
	                            RunFileReader probeReader = probeWriter.createReader();
	                            probeReader.open();
	                            while (probeReader.nextFrame(inBuffer)) {
	                                state.joiner.join(inBuffer, writer);
	                                inBuffer.clear();
	                            }
	                            probeReader.close();
                            }
                            state.joiner.write(writer);
                            state.joiner.closeJoin(writer);
                        	System.out.println("***\tClose8");
                        }
                    }
                    writer.close();
                }

                private void closeWriter(int i) throws HyracksDataException {
                    RunFileWriter writer = probeWriters[i];
                    if (writer != null) {
                        writer.close();
                    }
                }

                private void write(int i, ByteBuffer head) throws HyracksDataException {
                    RunFileWriter writer = probeWriters[i];
                    if (writer == null) {
                        FileReference file = ctx.createManagedWorkspaceFile(PartitionAndJoinActivityNode.class
                                .getSimpleName());
                        writer = new RunFileWriter(file, ctx.getIOManager());
                        writer.open();
                        probeWriters[i] = writer;
                    }
                    writer.nextFrame(head);
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }
            };
            return op;
        }
    }
}