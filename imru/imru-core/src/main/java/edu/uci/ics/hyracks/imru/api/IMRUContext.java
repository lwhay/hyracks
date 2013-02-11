package edu.uci.ics.hyracks.imru.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class IMRUContext {
    private String operatorName;
    private NodeControllerService nodeController;

    private String nodeId;
    private IHyracksTaskContext ctx;

    public IMRUContext(IHyracksTaskContext ctx) {
        this(ctx, null);
    }

    public IMRUContext(IHyracksTaskContext ctx, String operatorName) {
        this.ctx = ctx;
        this.operatorName = operatorName;
        IHyracksJobletContext jobletContext = ctx.getJobletContext();
        if (jobletContext instanceof Joblet) {
            this.nodeController = ((Joblet) jobletContext).getNodeController();
            this.nodeId = nodeController.getId();
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public String getOperatorName() {
        return operatorName;
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

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }
}
