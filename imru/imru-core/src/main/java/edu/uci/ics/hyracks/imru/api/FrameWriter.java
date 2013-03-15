package edu.uci.ics.hyracks.imru.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class FrameWriter {
    IFrameWriter writer;

    public FrameWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    public void writeFrame(ByteBuffer frame) throws HyracksDataException {
        FrameUtils.flushFrame(frame, writer);
    }
}
