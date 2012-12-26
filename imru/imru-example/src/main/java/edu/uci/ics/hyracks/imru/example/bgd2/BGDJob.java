package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;
import edu.uci.ics.hyracks.imru.example.bgd.R;

public class BGDJob implements IMRUJob<LinearModel, LossGradient> {
    int features;
    int rounds;

    public BGDJob(int features, int rounds) {
        this.features = features;
        this.rounds = rounds;
    }

    @Override
    public int getCachedDataFrameSize() {
        return 4 * 1024;
    }

    @Override
    public LinearModel initModel() {
        return new LinearModel(features, rounds);
    }

    @Override
    public LossGradient map(Iterator<ByteBuffer> input, LinearModel model,
            int cachedDataFrameSize) throws HyracksDataException {
        LossGradient lossGradientMap = new LossGradient(model.numFeatures);
        FrameTupleAccessor accessor = new FrameTupleAccessor(
                cachedDataFrameSize, RecordDescriptorUtils
                        .getDummyRecordDescriptor(2));
        LinearExample example = new LinearExample();
        while (input.hasNext()) {
            ByteBuffer buf = input.next();
            accessor.reset(buf);
            int tupleCount = accessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                example.reset(accessor, i);
                float innerProduct = example.dot(model.weights);
                float diff = (example.getLabel() - innerProduct);
                lossGradientMap.loss += diff * diff; // Use L2 loss
                // function.
                example.computeGradient(model.weights, innerProduct,
                        lossGradientMap.gradient);
            }
        }
        return lossGradientMap;
    }

    @Override
    public void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        int activeFeatures = 0;
        try {
            Pattern whitespacePattern = Pattern.compile("\\s+");
            Pattern labelFeaturePattern = Pattern.compile("[:=]");
            String line;
            boolean firstLine = true;
            while (true) {
                tb.reset();
                if (firstLine) {
                    long start = System.currentTimeMillis();
                    line = reader.readLine();
                    long end = System.currentTimeMillis();
                    // LOG.info("First call to reader.readLine() took " + (end -
                    // start) + " milliseconds");
                    firstLine = false;
                } else {
                    line = reader.readLine();
                }
                if (line == null) {
                    break;
                }
                String[] comps = whitespacePattern.split(line, 2);

                // Label
                // Ignore leading plus sign
                if (comps[0].charAt(0) == '+') {
                    comps[0] = comps[0].substring(1);
                }

                int label = Integer.parseInt(comps[0]);
                dos.writeInt(label);
                tb.addFieldEndOffset();
                Scanner scan = new Scanner(comps[1]);
                scan.useDelimiter(",|\\s+");
                while (scan.hasNext()) {
                    String[] parts = labelFeaturePattern.split(scan.next());
                    int index = Integer.parseInt(parts[0]);
                    if (index > features) {
                        throw new IndexOutOfBoundsException("Feature index "
                                + index
                                + " exceed the declared number of features ("
                                + features + ")");
                    }
                    // Ignore leading plus sign.
                    if (parts[1].charAt(0) == '+') {
                        parts[1] = parts[1].substring(1);
                    }
                    float value = Float.parseFloat(parts[1]);
                    dos.writeInt(index);
                    dos.writeFloat(value);
                    activeFeatures++;
                }
                dos.writeInt(-1); // Marks the end of the sparse array.
                tb.addFieldEndOffset();
                if (!appender.append(tb.getFieldEndOffsets(),
                        tb.getByteArray(), 0, tb.getSize())) {
                    FrameUtils.flushFrame(frame, writer);
                    appender.reset(frame, true);
                    if (!appender.append(tb.getFieldEndOffsets(), tb
                            .getByteArray(), 0, tb.getSize())) {
                        // LOG.severe("Example too large to fit in frame: " +
                        // line);
                        throw new IllegalStateException();
                    }
                }
            }
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        // LOG.info("Parsed input partition containing " + activeFeatures +
        // " active features");
    }

    @Override
    public LossGradient reduce(Iterator<LossGradient> input)
            throws HyracksDataException {
        LossGradient loss = new LossGradient(features);
        while (input.hasNext()) {
            LossGradient buf = input.next();
            loss.loss += buf.loss;
            for (int i = 0; i < loss.gradient.length; i++) {
                loss.gradient[i] += buf.gradient[i];
            }
        }
        return loss;
    }

    @Override
    public boolean shouldTerminate(LinearModel model) {
        return model.roundsRemaining <= 0;
    }

    @Override
    public void update(Iterator<LossGradient> input, LinearModel model)
            throws HyracksDataException {
        LossGradient loss = new LossGradient(features);
        while (input.hasNext()) {
            LossGradient buf = input.next();
            loss.loss += buf.loss;
            for (int i = 0; i < loss.gradient.length; i++) {
                loss.gradient[i] += buf.gradient[i];
            }
        }
        // Update loss
        model.loss = loss.loss;
        model.loss += model.regularizationConstant * norm(model.weights.array);
        // Update weights
        for (int i = 0; i < model.weights.length; i++) {
            model.weights.array[i] = (model.weights.array[i] - loss.gradient[i]
                    * model.stepSize)
                    * (1.0f - model.stepSize * model.regularizationConstant);
        }
        model.stepSize *= 0.9;
        model.roundsRemaining--;
    }

    public static double norm(float[] vec) {
        double norm = 0.0;
        for (double comp : vec) {
            norm += comp * comp;
        }
        return Math.sqrt(norm);
    }
}
