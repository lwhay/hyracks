package edu.uci.ics.hyracks.imru.dataflow;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IOneByOneReduceFunction;
import edu.uci.ics.hyracks.imru.api.IReassemblingReduceFunction;
import edu.uci.ics.hyracks.imru.api.IReduceFunction;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;

/**
 * Evaluates the reduce function in an iterative map reduce update job.
 *
 * @author Josh Rosen
 */
public class ReduceOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[1]);

    private final IIMRUJobSpecification<?> imruSpec;

    /**
     * Create a new ReduceOperatorDescriptor.
     *
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU Job specification
     */
    public ReduceOperatorDescriptor(JobSpecification spec, IIMRUJobSpecification<?> imruSpec) {
        super(spec, 1, 1);
        this.imruSpec = imruSpec;
        recordDescriptors[0] = dummyRecordDescriptor;
    }

    private static class ReduceOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private final IIMRUJobSpecification<?> imruSpec;
        private final ChunkFrameHelper chunkFrameHelper;
        private final List<List<ByteBuffer>> bufferedChunks;
        private final int partition;
        private IReduceFunction reduceFunction;

        public ReduceOperatorNodePushable(IHyracksTaskContext ctx, IIMRUJobSpecification<?> imruSpec, int partition) {
            this.imruSpec = imruSpec;
            this.chunkFrameHelper = new ChunkFrameHelper(ctx);
            this.bufferedChunks = new ArrayList<List<ByteBuffer>>();
            this.partition = partition;
        }

        @Override
        public void open() throws HyracksDataException {
            writer.open();
            reduceFunction = imruSpec.getReduceFunctionFactory().createReduceFunction(chunkFrameHelper.getContext());
            writer = chunkFrameHelper.wrapWriter(writer, partition);
            reduceFunction.setFrameWriter(writer);
        }

        @Override
        public void nextFrame(ByteBuffer encapsulatedChunk) throws HyracksDataException {
            ByteBuffer chunk = chunkFrameHelper.extractChunk(encapsulatedChunk);
            if (reduceFunction instanceof IOneByOneReduceFunction) {
                ((IOneByOneReduceFunction) reduceFunction).reduce(chunk);
            } else if (reduceFunction instanceof IReassemblingReduceFunction) {
                int senderPartition = chunkFrameHelper.getPartition(encapsulatedChunk);
                boolean isLastChunk = chunkFrameHelper.isLastChunk(encapsulatedChunk);
                enqueueChunk(chunk, senderPartition);
                if (isLastChunk) {
                    ((IReassemblingReduceFunction) reduceFunction).reduce(bufferedChunks.remove(senderPartition));
                }
            } else {
                throw new HyracksDataException("Unknown IReduceFunction interface");
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            reduceFunction.close();
            writer.close();
        }

        private void enqueueChunk(ByteBuffer chunk, int senderPartition) {
            if (bufferedChunks.size() <= senderPartition) {
                for (int i = bufferedChunks.size(); i <= senderPartition; i++) {
                    bufferedChunks.add(new LinkedList<ByteBuffer>());
                }
            }
            bufferedChunks.get(senderPartition).add(chunk);
        }

    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new ReduceOperatorNodePushable(ctx, imruSpec, partition);
    }

}
