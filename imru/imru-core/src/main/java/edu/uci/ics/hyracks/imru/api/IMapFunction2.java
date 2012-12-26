package edu.uci.ics.hyracks.imru.api;

import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMapFunction2 {
    void map(Iterator<ByteBuffer> input, IFrameWriter writer) throws HyracksDataException;
}
