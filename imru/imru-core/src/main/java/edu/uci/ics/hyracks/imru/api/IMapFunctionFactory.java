package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface IMapFunctionFactory<Model extends IModel> {
    IMapFunction createMapFunction(IHyracksTaskContext ctx, int cachedDataFrameSize, Model model);
}
