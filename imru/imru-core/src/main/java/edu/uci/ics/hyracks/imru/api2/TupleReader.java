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

package edu.uci.ics.hyracks.imru.api2;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class TupleReader extends DataInputStream {
    Iterator<ByteBuffer> input;
    FrameTupleAccessor accessor;
    ByteBufferInputStream in;
    int tupleId;
    int tupleCount;

    public TupleReader(Iterator<ByteBuffer> input, FrameTupleAccessor accessor, ByteBufferInputStream in) {
        super(in);
        this.input = input;
        this.accessor = accessor;
        this.in = in;
        tupleId = 0;
        tupleCount = 0;
    }

    public boolean hasNextTuple() {
        if (tupleId + 1 < tupleCount)
            return true;
        return input.hasNext();
    }

    public boolean nextTuple() {
        tupleId++;
        while (tupleId >= tupleCount) {
            if (!input.hasNext())
                return false;
            ByteBuffer buf = input.next();
            accessor.reset(buf);
            tupleId = 0;
            tupleCount = accessor.getTupleCount();
        }
        seekToField(0);
        return true;
    }

    public void seekToField(int fieldId) {
        int startOffset = accessor.getFieldSlotsLength() + accessor.getTupleStartOffset(tupleId)
                + accessor.getFieldStartOffset(tupleId, fieldId);
        // R.printHex(0, accessor.getBuffer().array(), startOffset, 256);
        // R.p("offset " + startOffset);
        in.setByteBuffer(accessor.getBuffer(), startOffset);
    }
}
