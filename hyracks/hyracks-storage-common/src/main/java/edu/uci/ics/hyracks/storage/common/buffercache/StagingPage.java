package edu.uci.ics.hyracks.storage.common.buffercache;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StagingPage implements ICachedPage {

    final ByteBuffer buffer;
    final AtomicBoolean dirty;
    final AtomicInteger pinCount;
    final ReadWriteLock latch;
    volatile long dpid;
    CachedPage next;
    volatile boolean valid;

    public StagingPage(int pagesize) {
        buffer = ByteBuffer.allocate(pagesize);
        pinCount = new AtomicInteger();
        dirty = new AtomicBoolean();
        latch = new ReentrantReadWriteLock(true);
        dpid = -1;
        valid = false;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void acquireReadLatch() {
        latch.readLock().lock();
    }

    @Override
    public void releaseReadLatch() {
        latch.readLock().unlock();

    }

    @Override
    public void acquireWriteLatch() {
        latch.writeLock().lock();

    }

    @Override
    public void releaseWriteLatch(boolean markDirty) {
        if (markDirty) {
            if (dirty.compareAndSet(false, true)) {
                pinCount.incrementAndGet();
            }
        }
        latch.writeLock().unlock();
    }

}
