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

package edu.uci.ics.hyracks.imru.example.digitRecognition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api2.DataWriter;
import edu.uci.ics.hyracks.imru.api2.IIMRUJob;
import edu.uci.ics.hyracks.imru.api2.IMRUDataException;
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;
import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Core IMRU application specific code. The dataflow is
 * parse->map->reduce->update
 * 
 * @author wangrui
 */
public class Job implements IIMRUJob<NeuralNetwork, Data, Result> {
    /**
     * Frame size must be large enough to store at least one tuple
     */
    @Override
    public int getCachedDataFrameSize() {
        return 10240;
    }

    /**
     * Parse input data and output tuples
     */
    @Override
    public void parse(IMRUContext ctx, InputStream input, DataWriter<Data> output) throws IOException {
        try {
            Rt.p("parse data");
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
                    images = MNIST.readImages(new DataInputStream(new ByteArrayInputStream(memory.toByteArray())));
                } else if (entry.getName().startsWith("labels")) {
                    ByteArrayOutputStream memory = new ByteArrayOutputStream();
                    CreateHar.copy(zip, memory);
                    labels = MNIST.readLabel(new DataInputStream(new ByteArrayInputStream(memory.toByteArray())));
                }
            }
            if (images == null || labels == null)
                throw new IMRUDataException("invalid input data");
            for (int i = 0; i < labels.length; i++) {
                Data data = new Data();
                data.label = labels[i];
                data.image = images[i];
            }
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    /**
     * For each tuple, return one result. Or by using nextTuple(), return one
     * result after processing multiple tuples.
     */
    @Override
    public Result map(IMRUContext ctx, Iterator<Data> input, NeuralNetwork model) throws IOException {
        Result weight = new Result(model.layers);
        NNResult result = new NNResult(model.layers);
        int numPatternsSampled = 0;

        while (input.hasNext()) {
            Data data = input.next();
            model.calculate(result, data.image);
            int result2 = model.result(result);
            double loss = model.loss(result, data.label);
            model.backpropagate(result, data.label);
            weight.total++;
            if (result2 == data.label)
                weight.correct++;
            weight.loss += loss;
            // R.p("map "+result.sum());
            weight.add(result);

            if (numPatternsSampled < 100) {
                model.calculate(result, data.image);
                model.backpropagateSecondDervatives(result, weight);
                numPatternsSampled++;
            }
        }

        // R.p("map end " + weight.correct + " " + weight.total + " "
        // + weight.sum());
        return weight;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public Result reduce(IMRUContext ctx, Iterator<Result> input) throws IMRUDataException {
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
    public NeuralNetwork update(IMRUContext ctx, Iterator<Result> input, NeuralNetwork model)
            throws IMRUDataException {
        int total = 0;
        int correct = 0;
        double loss = 0;
        double weightChange = 0;
        Result result = reduce(ctx, input);
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
        Rt.p("error rate: " + model.errorRate + "\tloss:" + loss + "\tweightChange: " + weightChange);
        model.learningRate *= model.learningRateDecrease;
        model.roundsRemaining--;
        return model;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(NeuralNetwork model) {
        return model.roundsRemaining == 0 || model.errorRate <= 0.01;
    }
}
