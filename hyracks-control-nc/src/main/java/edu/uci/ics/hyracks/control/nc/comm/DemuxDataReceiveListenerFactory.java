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
package edu.uci.ics.hyracks.control.nc.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListener;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListenerProvider;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DemuxDataReceiveListenerFactory implements IDataReceiveListenerProvider, IConnectionDemultiplexer,
        IDataReceiveListener {
    private static final Logger LOGGER = Logger.getLogger(DemuxDataReceiveListenerFactory.class.getName());

    private final IHyracksContext ctx;
    private final BitSet freeSlotBits;
    private final BitSet readyBits;
    private final List<ConnectionEntry> connections;
    private int openConnectionsCount;
    private int lastReadSender;
    private UUID jobId;
    private UUID stageId;

    public DemuxDataReceiveListenerFactory(IHyracksContext ctx, UUID jobId, UUID stageId) {
        this.ctx = ctx;
        this.jobId = jobId;
        this.stageId = stageId;
        freeSlotBits = new BitSet();
        readyBits = new BitSet();
        connections = new ArrayList<ConnectionEntry>();
        openConnectionsCount = 0;
        lastReadSender = 0;
    }

    @Override
    public IDataReceiveListener getDataReceiveListener() {
        return this;
    }

    @Override
    public synchronized void dataReceived(IConnectionEntry entry) throws IOException {
        ConnectionEntry ce = (ConnectionEntry) entry;
        int slot = ce.getSlot();
        ByteBuffer buffer = entry.getReadBuffer();
        buffer.flip();
        int dataLen = buffer.remaining();
        if (dataLen >= ctx.getFrameSize() || entry.aborted()) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("NonDeterministicDataReceiveListener: frame received: " + slot + ": "
                        + entry.getConnectorId() + ":" + entry.getSenderPartition() + ":"
                        + entry.getReceiverPartition());
            }
            SelectionKey key = ce.getSelectionKey();
            if (key.isValid()) {
                int ops = key.interestOps();
                key.interestOps(ops & ~SelectionKey.OP_READ);
            }
            readyBits.set(slot);
            notifyAll();
            return;
        }
        buffer.compact();
    }

    @Override
    public void eos(IConnectionEntry entry) {
    }

    @Override
    public synchronized void addConnection(IConnectionEntry entry) {
        ConnectionEntry ce = (ConnectionEntry) entry;
        int slot = freeSlotBits.nextSetBit(0);
        if (slot < 0) {
            slot = connections.size();
            connections.add(ce);
        } else {
            connections.set(slot, ce);
        }
        freeSlotBits.clear(slot);
        ce.setSlot(slot);
        readyBits.clear(slot);
        ++openConnectionsCount;
    }

    @Override
    public synchronized IConnectionEntry findReadyEntry() {
        while (openConnectionsCount > 0 && readyBits.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
        lastReadSender = readyBits.nextSetBit(lastReadSender);
        if (lastReadSender < 0) {
            lastReadSender = readyBits.nextSetBit(0);
        }
        return connections.get(lastReadSender);
    }

    @Override
    public synchronized void unreadyEntry(IConnectionEntry entry) {
        ConnectionEntry ce = (ConnectionEntry) entry;
        readyBits.clear(ce.getSlot());
        SelectionKey key = ce.getSelectionKey();
        if (key.isValid()) {
            int ops = key.interestOps();
            key.interestOps(ops | SelectionKey.OP_READ);
            key.selector().wakeup();
        }
    }

    @Override
    public synchronized int closeEntry(IConnectionEntry entry) throws HyracksDataException {
        ConnectionEntry ce = (ConnectionEntry) entry;
        SelectionKey key = ce.getSelectionKey();
        key.cancel();
        ce.close();
        int slot = ce.getSlot();
        connections.set(slot, null);
        readyBits.clear(slot);
        freeSlotBits.set(slot);
        return --openConnectionsCount;
    }

    @Override
    public synchronized int getOpenConnectionCount() {
        return openConnectionsCount;
    }

    @Override
    public UUID getJobId() {
        return jobId;
    }

    @Override
    public UUID getStageId() {
        return stageId;
    }
}