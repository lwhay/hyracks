package edu.uci.ics.hyracks.imru.data;

import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * Split binary data into many data frames
 * and then combined them together.
 * Each frame contains the source partition, target partition
 * and reply partition.Each node has multiple sender
 * and one receiver. Source partition is the sender partition.
 * Target partition and reply partition are receiver partition.
 * 
 * @author Rui Wang
 */
public class MergedFrames {
    public static final int HEADER = 20;
    public static final int SOURCE_OFFSET = 0;
    public static final int TARGET_OFFSET = 4;
    public static final int REPLY_OFFSET = 8;
    public static final int SIZE_OFFSET = 12;
    public static final int POSITION_OFFSET = 16;

    public int sourceParition;
    public int targetParition;
    public int replyPartition;
    public byte[] data;

    public static MergedFrames nextFrame(IHyracksTaskContext ctx,
            ByteBuffer buffer, Hashtable<Integer, LinkedList<ByteBuffer>> hash)
            throws HyracksDataException {
        if (buffer == null)
            return null;
        int frameSize = ctx.getFrameSize();
        LinkedList<ByteBuffer> queue = null;
        ByteBuffer frame = ctx.allocateFrame();
        frame.put(buffer.array(), 0, frameSize);
        int sourcePartition = buffer.getInt(SOURCE_OFFSET);
        queue = hash.get(sourcePartition);
        if (queue == null) {
            queue = new LinkedList<ByteBuffer>();
            hash.put(sourcePartition, queue);
        }
        queue.add(frame);
        int size = buffer.getInt(SIZE_OFFSET);
        int position = buffer.getInt(POSITION_OFFSET);
        //            Rt.p(position + "/" + size);
        if (position + frameSize - HEADER < size)
            return null;
        hash.remove(queue);
        byte[] bs = deserializeFromChunks(ctx.getFrameSize(), queue);
        MergedFrames merge = new MergedFrames();
        merge.data = bs;
        merge.sourceParition = sourcePartition;
        merge.targetParition = buffer.getInt(TARGET_OFFSET);
        merge.replyPartition = buffer.getInt(REPLY_OFFSET);
        return merge;
    }

    public static byte[] deserializeFromChunks(int frameSize,
            LinkedList<ByteBuffer> chunks) throws HyracksDataException {
        int curPosition = 0;
        byte[] bs = null;
        for (ByteBuffer buffer : chunks) {
            int size = buffer.getInt(SIZE_OFFSET);
            int position = buffer.getInt(POSITION_OFFSET);
            if (bs == null)
                bs = new byte[size];
            else if (size != bs.length)
                throw new Error();
            if (position != curPosition)
                throw new Error(position + " " + curPosition);
            int len = Math.min(bs.length - curPosition, frameSize - HEADER);
            System.arraycopy(buffer.array(), HEADER, bs, curPosition, len);
            curPosition += len;
            if (curPosition >= bs.length)
                break;
        }
        return bs;
    }

    public static void serializeToFrames(ByteBuffer frame, int frameSize,
            IFrameWriter writer, byte[] objectData, int sourcePartition,
            int targetPartition, int replyPartition)
            throws HyracksDataException {
        int position = 0;
        while (position < objectData.length) {
            frame.position(0);
            frame.putInt(sourcePartition);
            frame.putInt(targetPartition);
            frame.putInt(replyPartition);
            frame.putInt(objectData.length);
            frame.putInt(position);
            //            Rt.p(position);
            int length = Math.min(objectData.length - position, frameSize
                    - HEADER);
            frame.put(objectData, position, length);
            frame.position(frameSize);
            frame.flip();
            FrameUtils.flushFrame(frame, writer);
            position += length;
        }
    }
}
