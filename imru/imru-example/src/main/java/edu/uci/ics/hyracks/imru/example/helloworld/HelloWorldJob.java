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
import java.util.logging.Logger;

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
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;

public class HelloWorldJob implements
        IMRUJob<HelloWorldModel, HelloWorldIncrementalResult> {
    /**
     * Return initial model
     */
    @Override
    public HelloWorldModel initModel() {
        return new HelloWorldModel();
    }

    /**
     * Frame size must be large enough to store at least one tuple
     */
    @Override
    public int getCachedDataFrameSize() {
        return 256;
    }

    /**
     * Number of fields for each tuple
     */
    @Override
    public int getFieldCount() {
        return 3;
    }

    /**
     * Parse input data and output tuples
     */
    @Override
    public void parse(IHyracksTaskContext ctx, InputStream input,
            TupleWriter output) throws HyracksDataException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    input));
            String line = reader.readLine();
            reader.close();
            for (String s : line.split(" ")) {
                System.out.println("parse: " + s);
                // add one field
                output.writeUTF(s);
                output.finishField();
                // add another field
                output.writeUTF(s + "1");
                output.finishField();
                output.finishTuple();
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * For each tuple, return one result.
     * Or by using nextTuple(), return one result
     * after processing multiple tuples.
     */
    @Override
    public HelloWorldIncrementalResult map(TupleReader input,
            HelloWorldModel model, int cachedDataFrameSize) throws IOException {
        HelloWorldIncrementalResult result = new HelloWorldIncrementalResult();
        while (true) {
            input.seekToField(0);
            String word = input.readUTF();
            result.length += word.length();
            System.out.println("map: " + word + " -> " + result.length);
            if (!input.hasNextTuple())
                break;
            input.nextTuple();
        }
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public HelloWorldIncrementalResult reduce(
            Iterator<HelloWorldIncrementalResult> input)
            throws HyracksDataException {
        HelloWorldIncrementalResult combined = new HelloWorldIncrementalResult();
        StringBuilder sb = new StringBuilder();
        while (input.hasNext()) {
            HelloWorldIncrementalResult result = input.next();
            sb.append(result.length + "+");
            combined.length += result.length;
        }
        if (sb.length() > 0)
            sb.deleteCharAt(sb.length() - 1);
        System.out.println("reduce: " + sb + " -> " + combined.length);
        return combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public void update(Iterator<HelloWorldIncrementalResult> input,
            HelloWorldModel model) throws HyracksDataException {
        StringBuilder sb = new StringBuilder();
        int oldLength = model.totalLength;
        while (input.hasNext()) {
            HelloWorldIncrementalResult result = input.next();
            sb.append("+" + result.length);
            model.totalLength += result.length;
        }
        System.out.println("update: " + oldLength + sb + " -> "
                + model.totalLength);
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
