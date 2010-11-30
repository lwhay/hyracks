package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.util.List;
import java.util.UUID;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCBootstrap;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class CCBootstrap implements ICCBootstrap {
    private ICCApplicationContext appCtx;

    @Override
    public void start() throws Exception {
        final CentralInputSplitQueue queue = new CentralInputSplitQueue();
        appCtx.setDistributedState(queue);
        appCtx.addJobLifecycleListener(new IJobLifecycleListener() {
            @Override
            public void notifyJobStart(UUID jobId) {
            }

            @Override
            public void notifyJobFinish(UUID jobId) {
            }

            @Override
            public void notifyJobCreation(UUID jobId, JobSpecification jobSpec) {
                List<OnlineFileSplit> splits = null;
                // TODO: Add splits into the queue
                queue.addSplits(jobId, splits);
            }
        });
    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public void setApplicationContext(ICCApplicationContext appCtx) {
        this.appCtx = appCtx;
    }
}