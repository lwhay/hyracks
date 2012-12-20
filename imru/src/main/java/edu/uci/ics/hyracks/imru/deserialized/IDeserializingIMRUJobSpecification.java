package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.imru.api.IModel;

interface IDeserializingIMRUJobSpecification<T extends Serializable, Model extends IModel> {
    public int getCachedDataFrameSize();
    public ITupleParserFactory getTupleParserFactory();
    public IDeserializedMapFunctionFactory<T, Model> getDeserializedMapFunctionFactory();
    public IDeserializedReduceFunctionFactory<T> getDeserializedReduceFunctionFactory();
    public IDeserializedUpdateFunctionFactory<T, Model> getDeserializedUpdateFunctionFactory();
}
