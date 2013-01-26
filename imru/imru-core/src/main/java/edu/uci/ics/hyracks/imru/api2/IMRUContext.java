package edu.uci.ics.hyracks.imru.api2;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class IMRUContext {
    public IHyracksTaskContext ctx;

    public IMRUContext(IHyracksTaskContext ctx) {
        this.ctx = ctx;
    }
}
