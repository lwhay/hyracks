package edu.uci.ics.hyracks.imru.api;

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IReassemblingUpdateFunction extends IUpdateFunction {
    void update(List<ByteBuffer> chunks) throws HyracksDataException;
}
