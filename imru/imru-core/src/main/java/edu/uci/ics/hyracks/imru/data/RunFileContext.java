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

package edu.uci.ics.hyracks.imru.data;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.imru.util.DelegateHyracksTaskContext;

/**
 * Allows the run file to use large frames.
 */
public class RunFileContext extends DelegateHyracksTaskContext {

    private final int frameSize;

    /**
     * Construct a new RunFileContext.
     *
     * @param delegate
     *            The Hyracks Task Context used to create files.
     * @param frameSize
     *            The frame size used when writing to files.
     */
    public RunFileContext(IHyracksTaskContext delegate, int frameSize) {
        super(delegate);
        this.frameSize = frameSize;
    }

    @Override
    public int getFrameSize() {
        return frameSize;
    }

    @Override
    public ByteBuffer allocateFrame() {
        return ByteBuffer.allocate(getFrameSize());
    }

}