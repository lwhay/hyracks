package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public interface IIMRUJobSpecification<Model extends IModel> extends Serializable {
    /* Data Loading */
    /**
     * Returns the size of cached data frames.
     * <p>
     * Cached data frames may be larger than the frames sent over the
     * network in order to accommodate large, indivisible input
     * records.
     *
     * @return The size, in bytes, of cached input frames.
     */
    int getCachedDataFrameSize();

    /**
     * @return A tuple parser factory for parsing the input records.
     */
    ITupleParserFactory getTupleParserFactory();

    /* UDFs */
    IMapFunctionFactory<Model> getMapFunctionFactory();

    IReduceFunctionFactory getReduceFunctionFactory();

    IUpdateFunctionFactory<Model> getUpdateFunctionFactory();

    boolean shouldTerminate(Model model);

    /* Optimization */
    /* AggregationStrategy getAggregationStrategy */
}
