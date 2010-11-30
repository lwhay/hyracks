package edu.uci.ics.hyracks.examples.onlineaggregation;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;

public class CentralQueueAccessor {
    private static IInputSplitQueue QUEUE = null;

    public static void initialize(INCApplicationContext appCtx) {
        QUEUE = (IInputSplitQueue) appCtx.getDestributedState();
    }

    public static IInputSplitQueue getQueue() {
        return QUEUE;
    }
}