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

package edu.uci.ics.hyracks.imru.data;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * Tool for encapsulating frames to add chunking information.
 * <p>
 * To make it possible to re-assemble chunks at the operator level,
 * frames are required to have two additional fields: the number of
 * the partition that sent the frame, and a flag indicating whether
 * this is the last frame. This functionality could be pushed down to
 * the connector level, but this would greatly increase the code's
 * complexity.
 * <p>
 * Instead, UDFs are called with a context that allocates frames that
 * are smaller than the actual physical frame size. When these frames
 * are written to a wrapped frame writer, the frames are copied into
 * full-sized frames and prepended with the partition information.
 * <p>
 * To retain compatibility with connectors that expect frames to use
 * Hyracks' data frame format (as specified by FrameTupleAccessor),
 * the chunk is encapsulated in a frame with one tuple.
 *
 * @author Josh Rosen
 */
public class ChunkFrameHelper {
    private static final int BYTES_IN_INT = 4;
    private final IHyracksTaskContext ctx;
    private final int chunkSize;

    /**
     * Create a new ChunkFrameHelper.
     *
     * @param ctx
     *            The context used to allocate the frames used to hold
     *            the partition information and the frames presented
     *            to user code.
     */
    public ChunkFrameHelper(IHyracksTaskContext ctx) {
        this.ctx = ctx;
        // The maximum chunk size is based on subtracting the overhead
        // from the size of a full frame:
        //      4 bytes for field end offset
        //      4 bytes for sender's partition number
        //      4 bytes for "last chunk" flag.
        //      4 bytes for frame tuple count
        //      4 bytes for tuple end offset
        //   = 20 bytes total overhead
        this.chunkSize = ctx.getFrameSize() - 20;
    }

    /**
     * @return An IHyracksTaskContext to allocate the smaller chunk
     *         frames that will be encapsulated in full-size frames.
     */
    public IHyracksTaskContext getContext() {
        return new RunFileContext(ctx, chunkSize);
    }

    private void setUpFrame(ByteBuffer encapsulatedChunk) {
        // Set up the proper tuple structure in the frame:
        // Tuple count
        encapsulatedChunk.position(FrameHelper.getTupleCountOffset(ctx.getFrameSize()));
        encapsulatedChunk.putInt(1);
        assert encapsulatedChunk.getInt(FrameHelper.getTupleCountOffset(ctx.getFrameSize())) == 1;
        // Tuple end offset
        encapsulatedChunk.position(FrameHelper.getTupleCountOffset(ctx.getFrameSize()) - 4);
        encapsulatedChunk.putInt(FrameHelper.getTupleCountOffset(ctx.getFrameSize()) - 4);
        // Field end offset
        encapsulatedChunk.position(0);
        encapsulatedChunk.putInt(FrameHelper.getTupleCountOffset(ctx.getFrameSize()) - 4);
        encapsulatedChunk.position(0);
    }

    /**
     * Wrap an IFrameWriter to automatically encapsulate chunks.
     *
     * @param writer
     *            A writer accepting full-sized frames.
     * @param partition
     *            The partition number of the operator sending the
     *            chunks.
     * @return A wrapped IFrameWriter that performs the encapsulation.
     */
    public IFrameWriter wrapWriter(final IFrameWriter writer, final int partition) {
        return new IFrameWriter() {

            private ByteBuffer encapsulatedChunk = ctx.allocateFrame();
            private boolean waitingForFirstChunk = true;

            @Override
            public void open() throws HyracksDataException {
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                assert buffer.capacity() == chunkSize;
                // We need to wait to determine whether this is the
                // last chunk, so we buffer one frame.

                if (waitingForFirstChunk) {
                    setUpFrame(encapsulatedChunk);
                    waitingForFirstChunk = false;
                    encapsulatedChunk.position(3 * BYTES_IN_INT);
                    encapsulatedChunk.put(buffer);
                    return;
                }
                // Since we received a new frame, the previous frame
                // was not the last frame, so we can write it.
                // Flush the previous frame:
                encapsulatedChunk.position(BYTES_IN_INT);
                encapsulatedChunk.putInt(partition);
                encapsulatedChunk.putInt(0);
                encapsulatedChunk.position(0);
                FrameUtils.flushFrame(encapsulatedChunk, writer);
                // Buffer the new frame:
                setUpFrame(encapsulatedChunk);
                encapsulatedChunk.position(3 * BYTES_IN_INT);
                encapsulatedChunk.put(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                // If we have received at least one frame, then there
                // is a pending frame waiting to be flushed.
                if (!waitingForFirstChunk) {
                    encapsulatedChunk.position(BYTES_IN_INT);
                    encapsulatedChunk.putInt(partition);
                    encapsulatedChunk.putInt(1); // This is the last
                                                 // frame.
                    encapsulatedChunk.position(0);
                    FrameUtils.flushFrame(encapsulatedChunk, writer);
                }
                writer.close();
            }

        };
    }

    /**
     * Determine whether this is the last chunk from a partition.
     *
     * @param encapsulatedChunk
     *            An encapsulated chunk.
     * @return True if this is the last chunk, False otherwise.
     */
    public boolean isLastChunk(ByteBuffer encapsulatedChunk) {
        assert encapsulatedChunk.limit() == ctx.getFrameSize();
        return encapsulatedChunk.getInt(BYTES_IN_INT * 2) == 1;
    }

    /**
     * Get a chunk's sender partition number.
     *
     * @param encapsulatedChunk
     *            An encapsulated chunk.
     * @return The partition number of this chunk's sender.
     */
    public int getPartition(ByteBuffer encapsulatedChunk) {
        assert encapsulatedChunk.limit() == ctx.getFrameSize();
        return encapsulatedChunk.getInt(BYTES_IN_INT);
    }

    /**
     * Extract a chunk from its encapsulating frame.
     *
     * @param encapsulatedChunk
     *            An encapsulated chunk.
     * @return A new ByteBuffer containing the chunk, suitable for
     *         passing to user code.
     */
    public ByteBuffer extractChunk(ByteBuffer encapsulatedChunk) {
        assert encapsulatedChunk.limit() == ctx.getFrameSize();
        ByteBuffer chunk = ByteBuffer.allocate(chunkSize);
        chunk.put(encapsulatedChunk.array(), 3 * BYTES_IN_INT, chunkSize);
        return chunk;
    }

}
