package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IDeserializedMapFunction<T extends Serializable> {
    void open() throws HyracksDataException;
    T close() throws HyracksDataException;
    void map(ByteBuffer input) throws HyracksDataException;
}
