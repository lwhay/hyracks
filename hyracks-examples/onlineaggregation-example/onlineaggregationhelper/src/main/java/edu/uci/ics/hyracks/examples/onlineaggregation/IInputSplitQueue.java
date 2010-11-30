package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.rmi.Remote;
import java.util.UUID;

public interface IInputSplitQueue extends Remote {
    public MarshalledWritable<OnlineFileSplit> getNext(UUID jobId) throws Exception;
}