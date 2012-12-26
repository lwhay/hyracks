package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface IMapFunctionFactory<Model extends IModel> {
    boolean useAPI2();
    IMapFunction createMapFunction(IHyracksTaskContext ctx, int cachedDataFrameSize, Model model);
    IMapFunction2 createMapFunction2(IHyracksTaskContext ctx, int cachedDataFrameSize, Model model);
}
