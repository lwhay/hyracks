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
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListener;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;

public class ConnectionEntry implements IConnectionEntry {
    private static final Logger LOGGER = Logger.getLogger(ConnectionEntry.class.getName());

    private SocketChannel socketChannel;

    private final ByteBuffer readBuffer;

    private final ByteBuffer writeBuffer;

    private IDataReceiveListener recvListener;

    private int slot;

    private final SelectionKey key;

    private UUID jobId;

    private UUID stageId;

    private ConnectorDescriptorId cdId;

    private int senderPartition;

    private int receiverPartition;

    private boolean aborted;

    public ConnectionEntry(IHyracksContext ctx, SocketChannel socketChannel, SelectionKey key) {
        this.socketChannel = socketChannel;
        readBuffer = ctx.getResourceManager().allocateFrame();
        readBuffer.clear();
        writeBuffer = ctx.getResourceManager().allocateFrame();
        writeBuffer.clear();
        this.key = key;
    }

    public void setSlot(int slot) {
        this.slot = slot;
    }

    public int getSlot() {
        return slot;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public boolean dispatch(SelectionKey key) throws IOException {
        if (aborted) {
            recvListener.dataReceived(this);
        } else {
            if (key.isReadable()) {
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.finer("Before read: " + readBuffer.position() + " " + readBuffer.limit());
                }
                int bytesRead = socketChannel.read(readBuffer);
                if (bytesRead < 0) {
                    recvListener.eos(this);
                    return true;
                }
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.finer("After read: " + readBuffer.position() + " " + readBuffer.limit());
                }
                recvListener.dataReceived(this);
            } else if (key.isWritable()) {
                synchronized (this) {
                    writeBuffer.flip();
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.finer("Before write: " + writeBuffer.position() + " " + writeBuffer.limit());
                    }
                    int bytesWritten = socketChannel.write(writeBuffer);
                    if (bytesWritten < 0) {
                        return true;
                    }
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.finer("After write: " + writeBuffer.position() + " " + writeBuffer.limit());
                    }
                    if (writeBuffer.remaining() <= 0) {
                        int ops = key.interestOps();
                        key.interestOps(ops & ~SelectionKey.OP_WRITE);
                    }
                    writeBuffer.compact();
                    notifyAll();
                }
            } else {
                LOGGER.warning("Spurious event triggered: " + key.readyOps());
                return true;
            }
        }
        return false;
    }

    @Override
    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public synchronized void write(ByteBuffer buffer) {
        while (buffer.remaining() > 0) {
            while (writeBuffer.remaining() <= 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }
            int oldLimit = buffer.limit();
            buffer.limit(Math.min(oldLimit, writeBuffer.remaining()));
            writeBuffer.put(buffer);
            buffer.limit(oldLimit);
            int ops = key.interestOps();
            key.interestOps(ops | SelectionKey.OP_WRITE);
            key.selector().wakeup();
        }
    }

    public void setDataReceiveListener(IDataReceiveListener listener) {
        this.recvListener = listener;
    }

    public void close() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SelectionKey getSelectionKey() {
        return key;
    }

    public UUID getJobId() {
        return jobId;
    }

    public void setJobId(UUID jobId) {
        this.jobId = jobId;
    }

    public UUID getStageId() {
        return stageId;
    }

    public void setStageId(UUID stageId) {
        this.stageId = stageId;
    }

    public void abort() {
        aborted = true;
    }

    @Override
    public boolean aborted() {
        return aborted;
    }

    @Override
    public ConnectorDescriptorId getConnectorId() {
        return cdId;
    }

    public void setConnectorId(ConnectorDescriptorId cdId) {
        this.cdId = cdId;
    }

    @Override
    public int getSenderPartition() {
        return senderPartition;
    }

    public void setSenderPartition(int senderPartition) {
        this.senderPartition = senderPartition;
    }

    @Override
    public int getReceiverPartition() {
        return receiverPartition;
    }

    public void setReceiverPartition(int receiverPartition) {
        this.receiverPartition = receiverPartition;
    }
}