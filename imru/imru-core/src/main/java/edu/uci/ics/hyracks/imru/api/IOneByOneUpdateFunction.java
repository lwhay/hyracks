package edu.uci.ics.hyracks.imru.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IOneByOneUpdateFunction extends IUpdateFunction {
    void update(ByteBuffer chunk) throws HyracksDataException;
}
