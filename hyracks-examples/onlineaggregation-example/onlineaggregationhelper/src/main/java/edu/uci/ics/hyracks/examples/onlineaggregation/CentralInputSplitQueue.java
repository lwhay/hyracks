package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CentralInputSplitQueue extends UnicastRemoteObject implements IInputSplitQueue {
    private static final long serialVersionUID = 1L;

    private Map<UUID, LinkedList<OnlineFileSplit>> jobQueues;

    public CentralInputSplitQueue() throws RemoteException {
        jobQueues = new Hashtable<UUID, LinkedList<OnlineFileSplit>>();
    }

    @Override
    public MarshalledWritable<OnlineFileSplit> getNext(UUID jobId) throws Exception {
        LinkedList<OnlineFileSplit> splitQueue = jobQueues.get(jobId);
        if (splitQueue == null) {
            return null;
        }
        OnlineFileSplit nextSplit = null;
        synchronized (splitQueue) {
            nextSplit = !splitQueue.isEmpty() ? splitQueue.remove() : null;
        }
        if (nextSplit != null) {
            MarshalledWritable<OnlineFileSplit> w = new MarshalledWritable<OnlineFileSplit>();
            w.set(nextSplit);
            return w;
        }
        return null;
    }

    public void addSplits(UUID jobId, List<OnlineFileSplit> splits) {
        LinkedList<OnlineFileSplit> splitQueue = new LinkedList<OnlineFileSplit>(splits);
        jobQueues.put(jobId, splitQueue);
    }
}