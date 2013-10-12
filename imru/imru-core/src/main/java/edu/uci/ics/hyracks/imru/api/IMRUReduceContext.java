package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class IMRUReduceContext extends IMRUContext {
    private int level;
    private boolean isLocal;

    public IMRUReduceContext(IHyracksTaskContext ctx, String operatorName, boolean isLocal, int level) {
        super(ctx, operatorName);
        this.isLocal = isLocal;
        this.level = level;
    }

    public boolean isLocalReducer() {
        return isLocal;
    }

    public int getReducerLevel() {
        return level;
    }
}
