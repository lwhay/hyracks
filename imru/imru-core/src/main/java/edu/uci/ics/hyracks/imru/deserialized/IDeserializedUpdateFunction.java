package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IDeserializedUpdateFunction<T extends Serializable> {
    void open() throws HyracksDataException;
    void close() throws HyracksDataException;
    void update(T input) throws HyracksDataException;
}
