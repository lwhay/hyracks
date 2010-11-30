package edu.uci.ics.hyracks.examples.onlineaggregation;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.application.INCBootstrap;

public class NCBootstrap implements INCBootstrap {
    INCApplicationContext appCtx;

    @Override
    public void start() throws Exception {
        CentralQueueAccessor.initialize(appCtx);
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public void setApplicationContext(INCApplicationContext appCtx) {
        this.appCtx = appCtx;
    }
}