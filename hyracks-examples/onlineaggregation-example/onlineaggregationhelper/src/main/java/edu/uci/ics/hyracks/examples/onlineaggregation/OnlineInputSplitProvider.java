package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.util.UUID;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class OnlineInputSplitProvider implements IOnlineInputSplitProvider {
    private UUID jobId;
    private IInputSplitQueue queue;

    public OnlineInputSplitProvider(UUID jobId, IInputSplitQueue queue) {
        this.jobId = jobId;
        this.queue = queue;
    }

    @Override
    public OnlineFileSplit next() throws HyracksDataException {
        try {
            MarshalledWritable<OnlineFileSplit> w = queue.getNext(jobId);
            if (w == null) {
                return null;
            }
            return w.get();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}