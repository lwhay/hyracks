package edu.uci.ics.hyracks.imru.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IOneByOneReduceFunction extends IReduceFunction {
    void reduce(ByteBuffer chunk) throws HyracksDataException;
}
