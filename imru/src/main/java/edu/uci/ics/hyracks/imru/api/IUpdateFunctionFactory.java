package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface IUpdateFunctionFactory<Model extends IModel> {
    IUpdateFunction createUpdateFunction(IHyracksTaskContext ctx, Model model);
}
