package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface IReduceFunctionFactory {
    IReduceFunction createReduceFunction(IHyracksTaskContext ctx);
}
