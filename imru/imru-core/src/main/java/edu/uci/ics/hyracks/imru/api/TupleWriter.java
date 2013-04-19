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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.imru.util.Rt;

public class TupleWriter implements DataOutput {
    FrameWriter writer;
    ByteBuffer frame;
    DataOutput dos;
    FrameTupleAppender appender;
    ArrayTupleBuilder tb;

    public TupleWriter(IHyracksTaskContext ctx, ByteBuffer frame, int nFields) {
        this.frame = frame;
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        tb = new ArrayTupleBuilder(nFields);
        dos = tb.getDataOutput();
        // create a new frame
        appender.reset(frame, true);
        tb.reset();
    }

    public TupleWriter(IMRUContext ctx, FrameWriter writer, int nFields) {
        this.writer = writer;
        frame = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        tb = new ArrayTupleBuilder(nFields);
        dos = tb.getDataOutput();
        // create a new frame
        appender.reset(frame, true);
        tb.reset();
    }

    public TupleWriter(IHyracksTaskContext ctx, IFrameWriter writer, int nFields) {
        this.writer = new FrameWriter(writer);
        frame = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        tb = new ArrayTupleBuilder(nFields);
        dos = tb.getDataOutput();
        // create a new frame
        appender.reset(frame, true);
        tb.reset();
    }

    public void finishField() {
        tb.addFieldEndOffset();
    }

    /**
     * Force to finish a frame
     * 
     * @throws HyracksDataException
     */
    public void finishFrame() throws HyracksDataException {
        if (writer != null)
            writer.writeFrame(frame);
        appender.reset(frame, true);
    }

    public void finishTuple() throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            writer.writeFrame(frame);
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                // LOG.severe("Example too large to fit in frame: " +
                // line);
                throw new HyracksDataException("Example too large to fit in frame");
            }
        }
        tb.reset();
    }

    public void dump() {
        Rt.p(Rt.getHex(0, frame.array(), 0, 256, false));
    }
    
    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            writer.writeFrame(frame);
            appender.reset(frame, true);
        }
    }

    @Override
    public void write(int b) throws IOException {
        dos.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        dos.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        dos.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        dos.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        dos.writeByte(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        dos.writeBytes(s);
    }

    @Override
    public void writeChar(int v) throws IOException {
        dos.writeChar(v);
    }

    @Override
    public void writeChars(String s) throws IOException {
        dos.writeChars(s);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        dos.writeDouble(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        dos.writeFloat(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        dos.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        dos.writeLong(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        dos.writeShort(v);
    }

    public void writeString(String s) throws IOException {
        dos.writeInt(s.length());
        dos.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        dos.writeUTF(s);
    }
}
