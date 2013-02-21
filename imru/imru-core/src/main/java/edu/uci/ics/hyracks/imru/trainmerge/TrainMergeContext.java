package edu.uci.ics.hyracks.imru.trainmerge;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;

public class TrainMergeContext<Model extends Serializable> extends IMRUContext {
    IFrameWriter writer;
    private int curPartition;
    IMRUConnection imruConnection;
    String jobId;

    public TrainMergeContext(IHyracksTaskContext ctx, String operatorName,
            IFrameWriter writer, int curPartition,
            IMRUConnection imruConnection, String jobId) {
        super(ctx, operatorName);
        this.writer = writer;
        this.curPartition = curPartition;
        this.imruConnection = imruConnection;
        this.jobId = jobId;
    }

    public int getCurPartition() {
        return curPartition;
    }

    public void setJobStatus(String status) {
        try {
            imruConnection.setStatus(jobId, status);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static final int BYTES_IN_INT = 4;

    public void send(int partition) throws IOException {
        Model model = (Model) getModel();
        byte[] bs = JavaSerializationUtils.serialize(model);
        ByteBuffer frame = ctx.allocateFrame();
        int frameSize = ctx.getFrameSize();
        serializeToFrames(frame, frameSize, writer, bs, partition);
    }

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
}
