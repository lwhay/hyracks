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

package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api2.DataWriter;
import edu.uci.ics.hyracks.imru.api2.IIMRUJob;
import edu.uci.ics.hyracks.imru.api2.IMRUDataException;
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;

public class BGDJob implements IIMRUJob<LinearModel, Data, LossGradient> {
    int features;

    public BGDJob(int features) {
        this.features = features;
    }

    @Override
    public int getCachedDataFrameSize() {
        return 1024 * 1024;
    }

    @Override
    public void parse(IMRUContext ctx, InputStream input, DataWriter<Data> output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        int activeFeatures = 0;
        Pattern whitespacePattern = Pattern.compile("\\s+");
        Pattern whitespacePattern2 = Pattern.compile(",|\\s+");
        Pattern labelFeaturePattern = Pattern.compile("[:=]");
        String line;
        boolean firstLine = true;
        while (true) {
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

            // Ignore leading plus sign
            if (comps[0].charAt(0) == '+')
                comps[0] = comps[0].substring(1);

            Data data = new Data();
            data.label = Integer.parseInt(comps[0]);
            String[] kvs = whitespacePattern2.split(comps[1]);
            data.fieldIds = new int[kvs.length];
            data.values = new float[kvs.length];
            for (int i = 0; i < kvs.length; i++) {
                String[] parts = labelFeaturePattern.split(kvs[i]);
                data.fieldIds[i] = Integer.parseInt(parts[0]);
                if (data.fieldIds[i] > features) {
                    throw new IndexOutOfBoundsException("Feature index " + data.fieldIds[i]
                            + " exceed the declared number of features (" + features + ")");
                }
                // Ignore leading plus sign.
                if (parts[1].charAt(0) == '+') {
                    parts[1] = parts[1].substring(1);
                }
                data.values[i] = Float.parseFloat(parts[1]);
                activeFeatures++;
            }
            output.addData(data);
        }
    }

    @Override
    public LossGradient map(IMRUContext ctx, Iterator<Data> input, LinearModel model) throws IOException {
        LossGradient lossGradientMap = new LossGradient(model.numFeatures);
        while (input.hasNext()) {
            Data data = input.next();
            float innerProduct = data.dot(model.weights);
            float diff = (data.label - innerProduct);
            lossGradientMap.loss += diff * diff; // Use L2 loss
            // function.
            data.computeGradient(model.weights, innerProduct, lossGradientMap.gradient);
        }
        return lossGradientMap;
    }

    @Override
    public LossGradient reduce(IMRUContext ctx, Iterator<LossGradient> input) throws IMRUDataException {
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
    public LinearModel update(IMRUContext ctx, Iterator<LossGradient> input, LinearModel model) throws IMRUDataException {
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
            model.weights.array[i] = (model.weights.array[i] - loss.gradient[i] * model.stepSize)
                    * (1.0f - model.stepSize * model.regularizationConstant);
        }
        model.stepSize *= 0.9;
        model.roundsRemaining--;
        return model;
    }

    public static double norm(float[] vec) {
        double norm = 0.0;
        for (double comp : vec) {
            norm += comp * comp;
        }
        return Math.sqrt(norm);
    }
}
