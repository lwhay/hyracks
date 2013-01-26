package edu.uci.ics.hyracks.imru.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class IMRUContext {
    private String operatorName;

    private IHyracksTaskContext ctx;

    public IMRUContext(IHyracksTaskContext ctx, String operatorName) {
        this.ctx = ctx;
        this.operatorName = operatorName;
    }

    public ByteBuffer allocateFrame() {
        return ctx.allocateFrame();
    }

    public int getFrameSize() {
        return ctx.getFrameSize();
    }

    public IHyracksJobletContext getJobletContext() {
        return ctx.getJobletContext();
    }

    public String getOperatorName() {
        return operatorName;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }
}
