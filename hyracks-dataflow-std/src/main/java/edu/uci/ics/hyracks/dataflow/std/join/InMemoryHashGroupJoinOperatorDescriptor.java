package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractTaskState;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class InMemoryHashGroupJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int BUILD_ACTIVITY_ID = 0;
    private static final int PROBE_ACTIVITY_ID = 1;
    private static final int OUTPUT_ACTIVITY_ID = 2;

    private static final long serialVersionUID = 1L;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] joinComparatorFactories;
    private final IBinaryComparatorFactory[] groupComparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final INullWriterFactory[] nullWriterFactories1;
    private final int tableSize;

    public InMemoryHashGroupJoinOperatorDescriptor(JobSpecification spec, int[] keys0, int[] keys1,
    		IBinaryHashFunctionFactory[] hashFunctionFactories, IBinaryComparatorFactory[] joinComparatorFactories,
    		IBinaryComparatorFactory[] groupComparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
    		RecordDescriptor recordDescriptor, INullWriterFactory[] nullWriterFactories1, int tableSize) {
        super(spec, 2, 1);
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.joinComparatorFactories = joinComparatorFactories;
        this.groupComparatorFactories = groupComparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        recordDescriptors[0] = recordDescriptor;
        this.nullWriterFactories1 = nullWriterFactories1;
        this.tableSize = tableSize;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        HashBuildActivityNode hba = new HashBuildActivityNode(new ActivityId(odId, BUILD_ACTIVITY_ID));
        HashProbeActivityNode hpa = new HashProbeActivityNode(new ActivityId(odId, PROBE_ACTIVITY_ID));
        OutputActivity oa = new OutputActivity(new ActivityId(odId, OUTPUT_ACTIVITY_ID));

        builder.addActivity(hba);
        builder.addSourceEdge(0, hba, 0);

        builder.addActivity(hpa);
        builder.addSourceEdge(1, hpa, 0);

        builder.addActivity(oa);
        builder.addTargetEdge(0, oa, 0);

        builder.addBlockingEdge(hba, hpa);
        builder.addBlockingEdge(hpa, oa);

    }

    public static class HashBuildTaskState extends AbstractTaskState {
        private InMemoryHashGroupJoin joiner;

        public HashBuildTaskState() {
        }

        private HashBuildTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private class HashBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public HashBuildActivityNode(ActivityId id) {
            super(id);
        }
        
        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);

            final IBinaryComparator[] comparators = new IBinaryComparator[joinComparatorFactories.length];
            for (int i = 0; i < joinComparatorFactories.length; ++i) {
                comparators[i] = joinComparatorFactories[i].createBinaryComparator();
            }
            final INullWriter[] nullWriters1 = new INullWriter[nullWriterFactories1.length];
                for (int i = 0; i < nullWriterFactories1.length; i++) {
                    nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
            }

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private HashBuildTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    ITuplePartitionComputerFactory hpcf0 = new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories);
                    ITuplePartitionComputerFactory hpcf1 = new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories);
                    state = new HashBuildTaskState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(),
                            partition));

                    state.joiner = new InMemoryHashGroupJoin(ctx, tableSize, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                            new FrameTupleAccessor(ctx.getFrameSize(), rd1), groupComparatorFactories, hpcf0 /*gByTpc0*/, hpcf1 /*gByTpc1*/, rd0, recordDescriptors[0], 
                                    aggregatorFactory, keys1, keys0, nullWriters1);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame();
                    FrameUtils.copy(buffer, copyBuffer);
                    try{
                    	state.joiner.build(copyBuffer);
                    } catch (IOException e){
                    	
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    ctx.setTaskState(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
            return op;
        }

    }

    private class HashProbeActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public HashProbeActivityNode(ActivityId id) {
            super(id);
        }
        
        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private HashBuildTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = (HashBuildTaskState) ctx.getTaskState(new TaskId(new ActivityId(getOperatorId(),
                            BUILD_ACTIVITY_ID), partition));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                	// Probe activity joiner
                    state.joiner.join(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    ctx.setTaskState(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    throw new HyracksDataException("InMemoryHashGroupJoinOperator has failed.");
                }
                
            };
            return op;
        }

	}

    private class OutputActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public OutputActivity(ActivityId id) {
            super(id);
        }
        
        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    HashBuildTaskState buildState = (HashBuildTaskState) ctx.getTaskState(new TaskId(
                            new ActivityId(getOperatorId(), BUILD_ACTIVITY_ID), partition));
                    InMemoryHashGroupJoin table = buildState.joiner;
                    writer.open();
                    try {
                        table.write(writer);
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    } finally {
                        writer.close();
                    }

                }
            };
        }
    }
}