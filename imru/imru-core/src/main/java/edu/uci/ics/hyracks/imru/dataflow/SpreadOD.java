package edu.uci.ics.hyracks.imru.dataflow;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.jobgen.SpreadGraph;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.util.Rt;

public class SpreadOD extends AbstractSingleActivityOperatorDescriptor {
    private static Logger LOG = Logger.getLogger(SpreadOD.class.getName());
    SpreadGraph.Level level;
    boolean first;
    boolean last;
    int roundNum;
    String modelName;
    IMRUConnection imruConnection;

    public SpreadOD(JobSpecification spec, SpreadGraph.Level[] levels, int level, String modelName,
            IMRUConnection imruConnection,int roundNum) {
        super(spec, level > 0 ? 1 : 0, level < levels.length - 1 ? 1 : 0);
        this.level = levels[level];
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        this.roundNum=roundNum;
        first = level == 0;
        last = level == levels.length - 1;
        if (!last)
            recordDescriptors[0] = new RecordDescriptor(new ISerializerDeserializer[1]);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        if (first) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    SpreadOD.this.nextFrame(ctx, writer, partition, null, null);
                }
            };
        } else if (last) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                LinkedList<ByteBuffer> queue = new LinkedList<ByteBuffer>();

                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    SpreadOD.this.nextFrame(ctx, writer, partition, buffer, queue);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                }
            };
        } else {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                LinkedList<ByteBuffer> queue = new LinkedList<ByteBuffer>();

                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    SpreadOD.this.nextFrame(ctx, writer, partition, buffer, queue);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                }
            };
        }
    }

    public static final int BYTES_IN_INT = 4;

    public static byte[] deserializeFromChunks(int frameSize, LinkedList<ByteBuffer> chunks)
            throws HyracksDataException {
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

    public static void serializeToFrames(ByteBuffer frame, int frameSize, IFrameWriter writer, byte[] objectData,
            int targetPartition) throws HyracksDataException {
        int position = 0;
        while (position < objectData.length) {
            frame.position(0);
            frame.putInt(targetPartition);
            frame.putInt(objectData.length);
            frame.putInt(position);
            //            Rt.p(position);
            int length = Math.min(objectData.length - position, frameSize - 3 * BYTES_IN_INT);
            frame.put(objectData, position, length);
            frame.position(frameSize);
            frame.flip();
            FrameUtils.flushFrame(frame, writer);
            position += length;
        }
    }

    public void nextFrame(IHyracksTaskContext ctx, IFrameWriter writer, int partition, ByteBuffer buffer,
            LinkedList<ByteBuffer> queue) throws HyracksDataException {
        int frameSize = ctx.getFrameSize();
        if (buffer != null) {
            ByteBuffer frame = ctx.allocateFrame();
            frame.put(buffer.array(), 0, frameSize);
            queue.add(frame);
            int size = buffer.getInt(4);
            int position = buffer.getInt(8);
            //            Rt.p(position + "/" + size);
            if (position + frameSize - 12 < size)
                return;
        }
        if (!last)
            writer.open();
        try {
            if (first != (queue == null))
                throw new Error();
            IMRUContext imruContext = new IMRUContext(ctx);
            String nodeId = imruContext.getNodeId();
            byte[] bs = null;
            if (first) {
                bs = imruConnection.downloadData(modelName);
                LOG.info("download model at " + nodeId + " round " + roundNum);
            } else {
                bs = deserializeFromChunks(ctx.getFrameSize(), queue);
            }
            Serializable model = (Serializable) JavaSerializationUtils.deserialize(bs,
                    SpreadOD.class.getClassLoader());
            INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
            IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
            context.model = model;
            context.modelAge = roundNum;
            SpreadGraph.Node node = level.nodes.get(partition);

            ByteBuffer frame = ctx.allocateFrame();
            for (SpreadGraph.Node n : node.subNodes) {
                //                        node.print(0);
                //                        Rt.p(to.nodes.get(partition).name + " " + new IMRUContext(ctx).getNodeId() + " to " + node.name);
                //                buffer.putInt(0, n.partitionInThisLevel);
                serializeToFrames(frame, frameSize, writer, bs, n.partitionInThisLevel);
                if (last)
                    throw new Error();
                //                writer.nextFrame(buffer);
            }
        } catch (Exception e) {
            if (!last)
                writer.fail();
            throw new HyracksDataException(e);
        } finally {
            if (!last)
                writer.close();
        }
    }
}
