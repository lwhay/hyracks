package edu.uci.ics.hyracks.imru.example.digitRecognition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;
import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.example.utils.R;

/**
 * Core IMRU application specific code. The dataflow is
 * parse->map->reduce->update
 * 
 * @author wangrui
 */
public class Job implements IMRUJob<NeuralNetwork, Result> {
    /**
     * Return initial model
     */
    @Override
    public NeuralNetwork initModel() {
        return new NeuralNetwork();
    }

    /**
     * Frame size must be large enough to store at least one tuple
     */
    @Override
    public int getCachedDataFrameSize() {
        return 10240;
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
            R.p("parse data");
            ZipInputStream zip = new ZipInputStream(input);
            byte[][][] images = null;
            byte[] labels = null;
            while (true) {
                ZipEntry entry = zip.getNextEntry();
                if (entry == null)
                    break;
                if (entry.getName().startsWith("images")) {
                    ByteArrayOutputStream memory = new ByteArrayOutputStream();
                    CreateHar.copy(zip, memory);
                    images = MNIST.readImages(new DataInputStream(
                            new ByteArrayInputStream(memory.toByteArray())));
                } else if (entry.getName().startsWith("labels")) {
                    ByteArrayOutputStream memory = new ByteArrayOutputStream();
                    CreateHar.copy(zip, memory);
                    labels = MNIST.readLabel(new DataInputStream(
                            new ByteArrayInputStream(memory.toByteArray())));
                }
            }
            if (images == null || labels == null)
                throw new HyracksDataException("invalid input data");
            for (int i = 0; i < labels.length; i++) {
                output.writeInt(i);
                output.finishField();
                output.writeByte(labels[i]);
                output.finishField();
                output.writeByte(images[i].length);
                output.writeByte(images[i][0].length);
                for (byte[] bs : images[i])
                    output.write(bs);
                output.finishField();
                output.finishTuple();
//                if (i > 1)
//                    break;
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * For each tuple, return one result. Or by using nextTuple(), return one
     * result after processing multiple tuples.
     */
    @Override
    public Result map(TupleReader input, NeuralNetwork model,
            int cachedDataFrameSize) throws IOException {
        Result weight = new Result(model.layers);
        NNResult result = new NNResult(model.layers);
        int numPatternsSampled = 0;

        while (true) {
            input.seekToField(0);
            int id = input.readInt();
            input.seekToField(1);
            int label = input.readByte() & 0xFF;
            input.seekToField(2);
            int h = input.readByte() & 0xFF;
            int w = input.readByte() & 0xFF;
            byte[][] image = new byte[h][w];
            for (int i = 0; i < h; i++) {
                int len = input.read(image[i]);
                if (len != w)
                    throw new Error();
            }

            model.calculate(result, image);
            int result2 = model.result(result);
            double loss = model.loss(result, label);
            model.backpropagate(result, label);
            weight.total++;
            if (result2 == label)
                weight.correct++;
            weight.loss += loss;
            // R.p("map "+result.sum());
            weight.add(result);

            if (numPatternsSampled < 100) {
                model.calculate(result, image);
                model.backpropagateSecondDervatives(result, weight);
                numPatternsSampled++;
            }

            if (!input.hasNextTuple())
                break;
            input.nextTuple();
        }

        // R.p("map end " + weight.correct + " " + weight.total + " "
        // + weight.sum());
        return weight;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public Result reduce(Iterator<Result> input) throws HyracksDataException {
        Result combined = null;
        while (input.hasNext()) {
            Result result = input.next();
            if (combined == null)
                combined = result;
            else {
                combined.add(result);
            }
        }
        // R.p("reduce " + combined.sum());
        return combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public void update(Iterator<Result> input, NeuralNetwork model)
            throws HyracksDataException {
        int total = 0;
        int correct = 0;
        double loss = 0;
        double weightChange = 0;
        Result result = reduce(input);
        total += result.total;
        correct += result.correct;
        loss += result.loss;
        weightChange += model.adjustWeights(result);
        for (int layerId = model.layers.length - 1; layerId > 0; layerId--) {
            NNLayer layer = model.layers[layerId];
            double[] ds = result.diagHessians[layerId];
            for (int i = 0; i < ds.length; i++) {
                if (ds[i] < 0)
                    throw new Error();
                layer.diagHessians[i] = ds[i] / result.numPatternsSampled;
            }
        }
        model.errorRate = (double) (total - correct) / total;
        R.p("error rate: " + model.errorRate + "\tloss:" + loss
                + "\tweightChange: " + weightChange);
        model.learningRate *= model.learningRateDecrease;
        model.roundsRemaining--;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(NeuralNetwork model) {
        return model.roundsRemaining == 0 || model.errorRate <= 0.01;
    }
}
