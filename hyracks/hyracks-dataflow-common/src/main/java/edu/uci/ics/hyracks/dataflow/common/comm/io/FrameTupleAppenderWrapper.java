/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class FrameTupleAppenderWrapper {
    private final FrameTupleAppender frameTupleAppender;
    private final ByteBuffer outputFrame;
    private final IFrameWriter outputWriter;

    public FrameTupleAppenderWrapper(FrameTupleAppender frameTupleAppender, ByteBuffer outputFrame,
            IFrameWriter outputWriter) {
        this.frameTupleAppender = frameTupleAppender;
        this.outputFrame = outputFrame;
        this.outputWriter = outputWriter;
    }

    public void open() throws HyracksDataException {
        outputWriter.open();
    }

    public void appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length)
            throws HyracksDataException {
        if (!frameTupleAppender.append(fieldSlots, bytes, offset, length)) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
            frameTupleAppender.reset(outputFrame, true);
            if (!frameTupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
    }

    public void flush() throws HyracksDataException {
        if (frameTupleAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputFrame, outputWriter);
        }
    }

    public void close() throws HyracksDataException {
        outputWriter.close();
    }

    public void fail() throws HyracksDataException {
        outputWriter.fail();
    }
}
