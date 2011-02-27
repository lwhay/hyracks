/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

public class CentralInputSplitQueue extends UnicastRemoteObject implements IInputSplitQueue {
    private static final long serialVersionUID = 1L;

    private Map<UUID, SplitHolder> jobQueues;

    public CentralInputSplitQueue() throws RemoteException {
        jobQueues = new Hashtable<UUID, SplitHolder>(); // Hashtable since we need a synchronized map.
    }

    @Override
    public MarshalledWritable<OnlineFileSplit> getNext(UUID jobId, int requestor, String location) throws Exception {
        SplitHolder splitHolder = jobQueues.get(jobId);
        if (splitHolder == null) {
            return null;
        }
        OnlineFileSplit nextSplit = null;
        synchronized (splitHolder) {
            List<StatsRecord> stats = splitHolder.stats.get(requestor);
            if (stats == null) {
                stats = new Vector<StatsRecord>(); // Vector since we need a synchronized list. Its handed to the serializer without a lock.
                splitHolder.stats.put(requestor, stats);
            } else {
                stats.get(stats.size() - 1).endTime = System.currentTimeMillis();
            }
            nextSplit = !splitHolder.splits.isEmpty() ? splitHolder.splits.remove() : null;
            if (nextSplit != null) {
                StatsRecord rec = new StatsRecord();
                rec.mapLocation = location;
                rec.blockId = nextSplit.blockId();
                rec.startTime = System.currentTimeMillis();
                rec.fileName = nextSplit.getPath().toString();
                rec.startOffset = nextSplit.getStart();
                rec.length = nextSplit.getLength();
                rec.locations = nextSplit.getLocations();
                stats.add(rec);
            }
        }
        if (nextSplit != null) {
            MarshalledWritable<OnlineFileSplit> w = new MarshalledWritable<OnlineFileSplit>();
            w.set(nextSplit);
            return w;
        }
        return null;
    }

    @Override
    public Map<Integer, List<StatsRecord>> getStatistics(UUID jobId) throws Exception {
        SplitHolder splitHolder = jobQueues.get(jobId);
        if (splitHolder == null) {
            return null;
        }
        synchronized (splitHolder) {
            return splitHolder.stats;
        }
    }

    public void addSplits(UUID jobId, List<OnlineFileSplit> splits) {
        jobQueues.put(jobId, new SplitHolder(splits));
    }

    private static class SplitHolder {
        LinkedList<OnlineFileSplit> splits;
        Map<Integer, List<StatsRecord>> stats;

        public SplitHolder(List<OnlineFileSplit> splits) {
            this.splits = new LinkedList<OnlineFileSplit>(splits);
            stats = new Hashtable<Integer, List<StatsRecord>>(); // Hashtable because its handed to the serializer without a lock.
        }
    }

    @Override
    public long getTimestamp() throws Exception {
        return System.currentTimeMillis();
    }
}