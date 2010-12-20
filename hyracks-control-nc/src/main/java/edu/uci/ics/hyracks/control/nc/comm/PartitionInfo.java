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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.comm.PartitionId;
import edu.uci.ics.hyracks.api.io.FileReference;

public class PartitionInfo {
    private final PartitionId partitionId;

    private final FileReference fRef;

    private AtomicInteger frameCount;

    private AtomicBoolean done;

    public PartitionInfo(PartitionId partitionId, FileReference fRef) {
        this.partitionId = partitionId;
        this.fRef = fRef;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }

    public FileReference getFileReference() {
        return fRef;
    }

    public void incrementFrameCount() {
        frameCount.incrementAndGet();
    }

    public int getFrameCount() {
        return frameCount.get();
    }

    public void setDone() {
        done.set(true);
    }

    public boolean isDone() {
        return done.get();
    }
}