package edu.uci.ics.hyracks.imru.api;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMapFunction {
    void open() throws HyracksDataException;
    void setFrameWriter(IFrameWriter writer);
    void close() throws HyracksDataException;
    void map(ByteBuffer inputData) throws HyracksDataException;
}
