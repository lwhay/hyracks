package edu.uci.ics.hyracks.imru.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.dataflow.IMRUDebugger;
import edu.uci.ics.hyracks.imru.dataflow.IMRUSerialize;
import edu.uci.ics.hyracks.imru.util.Rt;

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
    public static final int HEADER = 24;
    public static final int TAIL = 20;
    public static final int SOURCE_OFFSET = 4;
    public static final int TARGET_OFFSET = 8;
    public static final int REPLY_OFFSET = 12;
    public static final int SIZE_OFFSET = 16;
    public static final int POSITION_OFFSET = 20;

    public int sourceParition;
    public int targetParition;
    public int replyPartition;
    public byte[] data;

    public static MergedFrames nextFrame(IHyracksTaskContext ctx,
            ByteBuffer buffer, Hashtable<Integer, LinkedList<ByteBuffer>> hash)
            throws HyracksDataException {
        return nextFrame(ctx, buffer, hash, null);
    }

    public static MergedFrames nextFrame(IHyracksTaskContext ctx,
            ByteBuffer buffer, Hashtable<Integer, LinkedList<ByteBuffer>> hash,
            String debugInfo) throws HyracksDataException {
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
        //        if (position == 0)
        //            Rt.p(position + "/" + size);
        if (debugInfo != null)
            IMRUDebugger.sendDebugInfo("recv " + debugInfo + " " + position);

        if (position + frameSize - HEADER - TAIL < size)
            return null;
        hash.remove(queue);
        byte[] bs = deserializeFromChunks(ctx.getFrameSize(), queue);
        //        Rt.p("recv " + bs.length + " " + deserialize(bs));
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
                throw new HyracksDataException();
            if (position != curPosition) {
                Rt.p(size);
                Rt.p(position);
                Rt.p(buffer);
                //                System.exit(0);
                throw new HyracksDataException(position + " " + curPosition);
            }
            int len = Math.min(bs.length - curPosition, frameSize - HEADER
                    - TAIL);
            System.arraycopy(buffer.array(), HEADER, bs, curPosition, len);
            curPosition += len;
            if (curPosition >= bs.length)
                break;
        }
        return bs;
    }

    public static void serializeToFrames(IMRUContext ctx, IFrameWriter writer,
            byte[] objectData, int partition, String debugInfo)
            throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        serializeToFrames(ctx, frame, ctx.getFrameSize(), writer, objectData,
                partition, 0, partition, debugInfo);
    }

    public static Object deserialize(byte[] bytes) {
        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(bytes));
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void setUpFrame(IMRUContext ctx, ByteBuffer encapsulatedChunk) {
        // Set up the proper tuple structure in the frame:
        // Tuple count
        encapsulatedChunk.position(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()));
        encapsulatedChunk.putInt(1);
        // Tuple end offset
        encapsulatedChunk.position(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()) - 4);
        encapsulatedChunk.putInt(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()) - 4);
        // Field end offset
        encapsulatedChunk.position(0);
        encapsulatedChunk.putInt(FrameHelper.getTupleCountOffset(ctx
                .getFrameSize()) - 4);
        encapsulatedChunk.position(0);
    }

    public static void serializeToFrames(IMRUContext ctx, ByteBuffer frame,
            int frameSize, IFrameWriter writer, byte[] objectData,
            int sourcePartition, int targetPartition, int replyPartition,
            String debugInfo) throws HyracksDataException {
        int position = 0;
        //        Rt.p("send " + objectData.length + " " + deserialize(objectData));
        while (position < objectData.length) {
            if (ctx != null)
                setUpFrame(ctx, frame);
            frame.position(SOURCE_OFFSET);
            frame.putInt(sourcePartition);
            frame.putInt(targetPartition);
            frame.putInt(replyPartition);
            frame.putInt(objectData.length);
            frame.putInt(position);
            //            Rt.p(position);
            int length = Math.min(objectData.length - position, frameSize
                    - HEADER - TAIL);
            frame.put(objectData, position, length);
            //            frame.position(frameSize - TAIL);
            //            frame.putInt(0); //tuple count
            frame.position(frameSize);
            frame.flip();
            //            if (position == 0)
            //                Rt.p("send 0");
            //                Rt.p(frame);
            if (debugInfo != null)
                IMRUDebugger.sendDebugInfo("flush " + debugInfo + " "
                        + position);
            FrameUtils.flushFrame(frame, writer);
            position += length;
        }
    }
}
