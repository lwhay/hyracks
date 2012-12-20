package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IReduceFunction {
    void open() throws HyracksDataException;
    void setFrameWriter(IFrameWriter writer);
    void close() throws HyracksDataException;
}
