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

package edu.uci.ics.hyracks.imru.api;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.imru.util.Rt;

public class TupleReader extends DataInputStream {
    Iterator<ByteBuffer> input;
    IFrameTupleAccessor accessor;
    ByteBufferInputStream in;
    int tupleId;
    int tupleCount;

    public TupleReader(ByteBuffer input, int fieldCount) {
        this(input, new FrameTupleAccessor(input.limit(), new RecordDescriptor(
                new ISerializerDeserializer[fieldCount])),
                new ByteBufferInputStream());
    }

    public TupleReader(ByteBuffer input, int frameSize, int fieldCount) {
        this(input, new FrameTupleAccessor(frameSize, new RecordDescriptor(
                new ISerializerDeserializer[fieldCount])),
                new ByteBufferInputStream());
    }

    public TupleReader(Iterator<ByteBuffer> input, int frameSize, int fieldCount) {
        this(input, new FrameTupleAccessor(frameSize, new RecordDescriptor(
                new ISerializerDeserializer[fieldCount])),
                new ByteBufferInputStream());
    }

    public TupleReader(Iterator<ByteBuffer> input, FrameTupleAccessor accessor,
            ByteBufferInputStream in) {
        super(in);
        this.input = input;
        this.accessor = accessor;
        this.in = in;
        tupleId = 0;
        tupleCount = 0;
    }

    public TupleReader(final ByteBuffer input, FrameTupleAccessor accessor,
            ByteBufferInputStream in) {
        super(in);
        this.input = new Iterator<ByteBuffer>() {
            boolean first = true;

            @Override
            public void remove() {
            }

            public ByteBuffer next() {
                if (first) {
                    first = false;
                    return input;
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return first;
            }
        };
        this.accessor = accessor;
        this.in = in;
        tupleId = 0;
        tupleCount = 0;
    }

    public TupleReader(IFrameTupleAccessor accessor, int tupleId) {
        this(accessor, new ByteBufferInputStream(), tupleId);
    }

    public TupleReader(IFrameTupleAccessor accessor, ByteBufferInputStream in,
            int tupleId) {
        super(in);
        this.input = new Iterator<ByteBuffer>() {
            @Override
            public void remove() {
            }

            public ByteBuffer next() {
                return null;
            }

            @Override
            public boolean hasNext() {
                return false;
            }
        };
        this.accessor = accessor;
        this.in = in;
        this.tupleId = tupleId;
        tupleCount = accessor.getTupleCount();
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
        int startOffset = accessor.getFieldSlotsLength()
                + accessor.getTupleStartOffset(tupleId)
                + accessor.getFieldStartOffset(tupleId, fieldId);
//                Rt.p(accessor.getFieldSlotsLength() +" "+ accessor.getTupleStartOffset(tupleId)
//                        +" "+ accessor.getFieldStartOffset(tupleId, fieldId));
        in.setByteBuffer(accessor.getBuffer(), startOffset);
    }

    public String readString() throws IOException {
        int len = readInt();
        return readChars(len);
    }

    public String readChars(int length) throws IOException {
        char[] cs = new char[length];
        for (int i = 0; i < length; i++)
            cs[i] = readChar();
        return new String(cs);
    }

    public String readUTF(int length) throws IOException {
        byte[] cs = new byte[length];
        for (int i = 0; i < length; i++)
            cs[i] = (byte) read();
        return new String(cs);
    }

    public int getFieldCount() {
        return accessor.getFieldCount();
    }

    public int getFieldLength(int fieldId) {
        return accessor.getFieldLength(tupleId, fieldId);
    }

    public void dump() {
        Rt.p(Rt.getHex(0, accessor.getBuffer().array(), 0, 256, false));
    }
}
