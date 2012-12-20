package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IDeserializedReduceFunction<T extends Serializable> {
    void open() throws HyracksDataException;
    T close() throws HyracksDataException;
    void reduce(T input) throws HyracksDataException;
}
