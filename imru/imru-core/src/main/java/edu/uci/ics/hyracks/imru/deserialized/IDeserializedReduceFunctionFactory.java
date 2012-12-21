package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;

public interface IDeserializedReduceFunctionFactory<T extends Serializable> {
    IDeserializedReduceFunction<T> createReduceFunction();

}
