package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.io.IIOManager;

// This is just a dummy hyracks context for allocating frames for temporary
// results during inverted index searches.
// TODO: In the future we should use the real HyracksTaskContext to track
// frame usage.
public class DefaultHyracksCommonContext implements IHyracksCommonContext {
    private final int FRAME_SIZE = 32768;

    @Override
    public ByteBuffer allocateFrame() {
        return ByteBuffer.allocate(FRAME_SIZE);
    }

    @Override
    public int getFrameSize() {
        return FRAME_SIZE;
    }

    @Override
    public IIOManager getIOManager() {
        return null;
    }
}