package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;

import edu.uci.ics.hyracks.imru.api.IModel;
public interface IDeserializedUpdateFunctionFactory<T extends Serializable, Model extends IModel> {
    IDeserializedUpdateFunction<T> createUpdateFunction(Model model);

}
