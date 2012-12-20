package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;

import edu.uci.ics.hyracks.imru.api.IModel;

public interface IDeserializedMapFunctionFactory<T extends Serializable, Model extends IModel> {
    IDeserializedMapFunction<T> createMapFunction(Model model, int cachedDataFrameSize);
}
