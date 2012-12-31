package edu.uci.ics.hyracks.imru.api2;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class TupleWriter implements DataOutput {
    IFrameWriter writer;
    ByteBuffer frame;
    DataOutput dos;
    FrameTupleAppender appender;
    ArrayTupleBuilder tb;

    public TupleWriter(IHyracksTaskContext ctx, IFrameWriter writer, int nFields) {
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

    public void finishField() {
        tb.addFieldEndOffset();
    }

    /**
     * Force to finish a frame
     * @throws HyracksDataException
     */
    public void finishFrame() throws HyracksDataException {
        FrameUtils.flushFrame(frame, writer);
        appender.reset(frame, true);
    }

    public void finishTuple() throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb
                .getSize())) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                    tb.getSize())) {
                // LOG.severe("Example too large to fit in frame: " +
                // line);
                throw new HyracksDataException(
                        "Example too large to fit in frame");
            }
        }
        tb.reset();
    }

    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(frame, writer);
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

    @Override
    public void writeUTF(String s) throws IOException {
        dos.writeUTF(s);
    }
}
