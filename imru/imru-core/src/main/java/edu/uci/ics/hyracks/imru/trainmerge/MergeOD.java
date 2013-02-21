package edu.uci.ics.hyracks.imru.trainmerge;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Random;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.dataflow.SpreadOD;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

public class MergeOD<Model extends Serializable> extends
        AbstractSingleActivityOperatorDescriptor {
    private final static Logger LOGGER = Logger.getLogger(MergeOD.class
            .getName());
    IMRUConnection imruConnection;
    String modelName;
    TrainMergeJob<Model> trainMergejob;
    String jobId;
    int totalTrainPartitions;

    public MergeOD(JobSpecification spec, TrainMergeJob<Model> trainMergejob,
            IMRUConnection imruConnection, String modelName, String jobId,
            int totalTrainPartitions) {
        super(spec, 1, 0);
        this.imruConnection = imruConnection;
        this.modelName = modelName;
        this.trainMergejob = trainMergejob;
        this.jobId = jobId;
        this.totalTrainPartitions = totalTrainPartitions;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            final int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            Hashtable<Integer, LinkedList<ByteBuffer>> queue = new Hashtable<Integer, LinkedList<ByteBuffer>>();

            @Override
            public void open() throws HyracksDataException {
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                MergeOD.this.nextFrame(ctx, writer, partition, buffer, queue,
                        nPartitions);
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
                if (partition == 0) {
                    IMRUContext imruContext = new IMRUContext(ctx, "merge");
                    long start = System.currentTimeMillis();
                    Model model = (Model) imruContext.getModel();
                    try {
                        imruConnection.uploadModel(modelName, model);
                        imruConnection.finishJob(jobId);
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    long end = System.currentTimeMillis();
                    //                Rt.p(model);
                    LOGGER.info("uploaded model to CC " + (end - start)
                            + " milliseconds");
                }
            }
        };
    }

    public static final int BYTES_IN_INT = 4;

    public static byte[] deserializeFromChunks(int frameSize,
            LinkedList<ByteBuffer> chunks) throws HyracksDataException {
        int curPosition = 0;
        byte[] bs = null;
        for (ByteBuffer buffer : chunks) {
            int size = buffer.getInt(4);
            int position = buffer.getInt(8);
            if (bs == null)
                bs = new byte[size];
            else if (size != bs.length)
                throw new Error();
            //            Rt.p(position);
            if (position != curPosition)
                throw new Error(position + " " + curPosition);
            int len = Math.min(bs.length - curPosition, frameSize - 12);
            System.arraycopy(buffer.array(), 12, bs, curPosition, len);
            curPosition += len;
            if (curPosition >= bs.length)
                break;
        }
        return bs;
    }

    public static void serializeToFrames(ByteBuffer frame, int frameSize,
            IFrameWriter writer, byte[] objectData, int targetPartition)
            throws HyracksDataException {
        int position = 0;
        while (position < objectData.length) {
            frame.position(0);
            frame.putInt(targetPartition);
            frame.putInt(objectData.length);
            frame.putInt(position);
            //            Rt.p(position);
            int length = Math.min(objectData.length - position, frameSize - 3
                    * BYTES_IN_INT);
            frame.put(objectData, position, length);
            frame.position(frameSize);
            frame.flip();
            FrameUtils.flushFrame(frame, writer);
            position += length;
        }
    }

    public void nextFrame(IHyracksTaskContext ctx, IFrameWriter writer,
            int partition, ByteBuffer buffer,
            Hashtable<Integer, LinkedList<ByteBuffer>> hash, int nPartitions)
            throws HyracksDataException {
        MergedFrames frames = MergedFrames.nextFrame(ctx, buffer, hash);
        if (frames == null)
            return;
        try {
            TrainMergeContext context = new TrainMergeContext(ctx, "merge",
                    null, partition, totalTrainPartitions + partition,
                    partition, imruConnection, jobId);
            Serializable receivedObject = (Serializable) JavaSerializationUtils
                    .deserialize(frames.data, SpreadOD.class.getClassLoader());
            trainMergejob.receive(context, frames.replyPartition,
                    receivedObject);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
        }
    }
}