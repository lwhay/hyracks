package edu.uci.ics.hyracks.imru.data;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.imru.util.DelegateHyracksTaskContext;

/**
 * Allows the run file to use large frames.
 */
public class RunFileContext extends DelegateHyracksTaskContext {

    private final int frameSize;

    /**
     * Construct a new RunFileContext.
     *
     * @param delegate
     *            The Hyracks Task Context used to create files.
     * @param frameSize
     *            The frame size used when writing to files.
     */
    public RunFileContext(IHyracksTaskContext delegate, int frameSize) {
        super(delegate);
        this.frameSize = frameSize;
    }

    @Override
    public int getFrameSize() {
        return frameSize;
    }

    @Override
    public ByteBuffer allocateFrame() {
        return ByteBuffer.allocate(getFrameSize());
    }

}