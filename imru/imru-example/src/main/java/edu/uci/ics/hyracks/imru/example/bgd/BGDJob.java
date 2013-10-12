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

package edu.uci.ics.hyracks.imru.example.bgd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.IIMRUJob;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;

public class BGDJob implements IIMRUJob<Model, Data, Gradient> {
    int features;

    public BGDJob(int features) {
        this.features = features;
    }

    @Override
    public int getCachedDataFrameSize() {
        return 1024;
    }

    @Override
    public void parse(IMRUContext ctx, InputStream input,
            DataWriter<Data> output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        for (String line = reader.readLine(); line != null; line = reader
                .readLine()) {
            String[] ss = line.split(",|\\s+");
            Data data = new Data();
            data.label = Integer.parseInt(ss[0]) > 0 ? 1 : -1;
            data.fieldIds = new int[ss.length - 1];
            data.values = new float[ss.length - 1];
            for (int i = 1; i < ss.length; i++) {
                String[] kv = ss[i].split("[:=]");
                data.fieldIds[i - 1] = Integer.parseInt(kv[0]);
                data.values[i - 1] = Float.parseFloat(kv[1]);
            }
            output.addData(data);
        }
    }

    @Override
    public Gradient map(IMRUContext ctx, Iterator<Data> input, Model model)
            throws IOException {
        Gradient g = new Gradient(model.numFeatures);
        while (input.hasNext()) {
            Data data = input.next();
            float innerProduct = 0;
            for (int i = 0; i < data.fieldIds.length; i++)
                innerProduct += data.values[i]
                        * model.weights[data.fieldIds[i]];
            g.total++;
            if ((data.label > 0) != (innerProduct > 0)) {
                for (int i = 0; i < data.fieldIds.length; i++)
                    g.gradient[data.fieldIds[i]] += data.label * data.values[i];
            } else {
                g.correct++;
            }
        }
        return g;
    }

    @Override
    public Gradient reduce(IMRUContext ctx, Iterator<Gradient> input)
            throws IMRUDataException {
        Gradient g = new Gradient(features);
        while (input.hasNext()) {
            Gradient buf = input.next();
            g.correct += buf.correct;
            g.total += buf.total;
            for (int i = 0; i < g.gradient.length; i++)
                g.gradient[i] += buf.gradient[i];
        }
        return g;
    }

    @Override
    public Model update(IMRUContext ctx, Iterator<Gradient> input, Model model)
            throws IMRUDataException {
        Gradient g = reduce(ctx, input);
        model.error = 100f * (g.total - g.correct) / g.total;
        for (int i = 0; i < model.weights.length; i++)
            model.weights[i] += g.gradient[i] / g.total * model.stepSize;
        model.stepSize *= 0.9;
        if (model.error < 0.0001)
            model.roundsRemaining = 0;
        else
            model.roundsRemaining--;
        model.roundsCompleted++;
        return model;
    }

    @Override
    public boolean shouldTerminate(Model model) {
        return model.roundsRemaining <= 0;
    }
}
