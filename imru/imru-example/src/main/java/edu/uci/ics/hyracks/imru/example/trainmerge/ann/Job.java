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

package edu.uci.ics.hyracks.imru.example.trainmerge.ann;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api2.DataWriter;
import edu.uci.ics.hyracks.imru.api2.IIMRUJob;
import edu.uci.ics.hyracks.imru.api2.IMRUDataException;
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;
import edu.uci.ics.hyracks.imru.example.utils.CreateHar;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeContext;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeJob;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Core IMRU application specific code. The dataflow is
 * parse->map->reduce->update
 * 
 * @author wangrui
 */
public class Job implements TrainMergeJob<NeuralNetwork> {
    /**
     * Repeat the training for this many turns
     */
    int turns;

    /**
     * After backpropagation this times, send out the model to merge.
     */
    int transferThreshold;

    public Job(int turns, int transferThreshold) {
        this.turns = turns;
        this.transferThreshold = transferThreshold;
    }

    @Override
    public void train(TrainMergeContext<NeuralNetwork> context,
            IMRUFileSplit input, NeuralNetwork model, int totalNodes)
            throws IOException {
        Random random = new Random();
        try {
            String path = input.getPath();
            int start = Integer.parseInt(input.getParameter("start"));
            int len = Integer.parseInt(input.getParameter("len"));
            File imageFile = new File(path, "train-images.idx3-ubyte");
            File labelFile = new File(path, "train-labels.idx1-ubyte");
            byte[] labels = MNIST.readLabel(new DataInputStream(
                    new FileInputStream(labelFile)), start, len);
            byte[][][] images = MNIST.readImages(new DataInputStream(
                    new FileInputStream(imageFile)), start, len);
            NNResult result = new NNResult(model.layers);
            for (int turnId = 0; turnId < turns; turnId++) {
                int backpropagateSecondDervativesSamples = len / 100;
                if (backpropagateSecondDervativesSamples < 1)
                    backpropagateSecondDervativesSamples = 1;
                for (int i = 0; i < backpropagateSecondDervativesSamples; i++) {
                    int id = random.nextInt(len);
                    model.calculate(result, images[id]);
                    model.backpropagateSecondDervatives(result);
                }
                for (int layerId = model.layers.length - 1; layerId > 0; layerId--) {
                    NNLayer layer = model.layers[layerId];
                    double[] ds = layer.diagHessians;
                    for (int i = 0; i < ds.length; i++) {
                        if (ds[i] < 0)
                            throw new Error();
                        layer.diagHessians[i] = ds[i]
                                / backpropagateSecondDervativesSamples;
                    }
                }

                int total = 0;
                int correct = 0;
                double loss = 0;
                long trainTime = 0;
                long transferTime = 0;
                int transferCount = 0;
                //backpropagate time is 6ms
                //transfer time is 60ms
                for (int i = 0; i < labels.length; i++) {
                    long startTime = System.nanoTime();
                    model.calculate(result, images[i]);
                    int result2 = model.result(result);
                    loss += model.loss(result, labels[i]);
                    model.backpropagate(result, labels[i]);
                    if (result2 == labels[i])
                        correct++;
                    total++;
                    trainTime += (System.nanoTime() - startTime);
                    if (i % transferThreshold == transferThreshold - 1) {
                        startTime = System.nanoTime();
                        context.send(random.nextInt(totalNodes));
                        transferTime += (System.nanoTime() - startTime);
                        transferCount++;
                    }
                }
                model.errorRate = (double) (total - correct) * 100 / total;
                Rt.p("error rate: %.2f loss=%f bpTime=%.2fms transferTime=%.2fms",
                        model.errorRate, loss, (double) trainTime / 1000000
                                / total, (double) transferTime / 1000000
                                / transferCount);
            }
        } catch (IOException e) {
            throw new IMRUDataException(e);
        }
    }

    @Override
    public NeuralNetwork merge(TrainMergeContext<NeuralNetwork> context,
            NeuralNetwork model, NeuralNetwork receivedModel)
            throws IOException {
        // merge time is below 0.15ms
        //        long startTime = System.nanoTime();
        model.merge(receivedModel);
        //        long mergeTime = (System.nanoTime() - startTime);
        //        Rt.p("mergeTime=%.2fms", (double) mergeTime / 1000000);
        return model;
    }
}
