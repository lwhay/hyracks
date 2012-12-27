package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;
import edu.uci.ics.hyracks.imru.example.utils.R;

public class HelloWorldJob implements
        IMRUJob<HelloWorldModel, HelloWorldIncrementalResult> {
    static AtomicInteger nextId = new AtomicInteger();

    public HelloWorldJob() {
    }

    @Override
    public HelloWorldModel initModel() {
        return new HelloWorldModel();
    }

    @Override
    public int getCachedDataFrameSize() {
        return 256;
    }

    /**
     * Parse input data and create frames
     */
    @Override
    public void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in));
            String line = reader.readLine();
            reader.close();
            for (String s : line.split(" ")) {
                // create a new frame
                appender.reset(frame, true);

                tb.reset();
                // add one field
                dos.writeUTF(s);
                tb.addFieldEndOffset();
                // add another field
                dos.writeUTF(s + "_copy");
                tb.addFieldEndOffset();
                if (!appender.append(tb.getFieldEndOffsets(),
                        tb.getByteArray(), 0, tb.getSize())) {
                    // if frame can't hold this tuple
                    throw new IllegalStateException(
                            "Example too large to fit in frame: " + line);
                }
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public HelloWorldIncrementalResult map(Iterator<ByteBuffer> input,
            HelloWorldModel model, int cachedDataFrameSize)
            throws HyracksDataException {
        int id = nextId.getAndIncrement();
        HelloWorldIncrementalResult mapResult;
        IFrameTupleAccessor accessor;
        R.p("openMap" + id + " model=" + model.totalLength);
        mapResult = new HelloWorldIncrementalResult();
        accessor = new FrameTupleAccessor(cachedDataFrameSize,
                new RecordDescriptor(new ISerializerDeserializer[2]));
        while (input.hasNext()) {
            ByteBuffer buf = input.next();
            try {
                accessor.reset(buf);
                int tupleCount = accessor.getTupleCount();
                ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream di = new DataInputStream(bbis);
                for (int i = 0; i < tupleCount; i++) {
                    int fieldId = 0;
                    int startOffset = accessor.getFieldSlotsLength()
                            + accessor.getTupleStartOffset(i)
                            + accessor.getFieldStartOffset(i, fieldId);
                    bbis.setByteBuffer(accessor.getBuffer(), startOffset);
                    String word = di.readUTF();
                    R.p("map%d read frame: %s", id, word);
                    mapResult.length += word.length();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        R.p("closeMap" + id + " output=" + mapResult.length);
        return mapResult;
    }

    @Override
    public HelloWorldIncrementalResult reduce(
            Iterator<HelloWorldIncrementalResult> input)
            throws HyracksDataException {
        int id = nextId.getAndIncrement();
        HelloWorldIncrementalResult reduceResult;
        R.p("openReduce" + id);
        reduceResult = new HelloWorldIncrementalResult();
        while (input.hasNext()) {
            HelloWorldIncrementalResult result = input.next();
            R.p("reduce" + id + " input=" + result.length);
            reduceResult.length += result.length;
        }
        R.p("closeReduce" + id + " output=" + reduceResult.length);
        return reduceResult;
    }

    @Override
    public void update(Iterator<HelloWorldIncrementalResult> input,
            HelloWorldModel model) throws HyracksDataException {
        int id = nextId.getAndIncrement();
        R.p("openUpdate" + id + " input=" + model.totalLength);
        while (input.hasNext()) {
            HelloWorldIncrementalResult result = input.next();
            R.p("update" + id);
            model.totalLength += result.length;
        }
        R.p("closeUpdate" + id + " output=" + model.totalLength);
        model.roundsRemaining--;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(HelloWorldModel model) {
        return model.roundsRemaining == 0;
    }
}
