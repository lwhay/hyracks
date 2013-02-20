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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * IMRU model which will be used in map() and updated in update()
 * 
 * @author wangrui
 */
public class NeuralNetwork implements Serializable {
    //    public static final long serialVersionUID=10000;
    public static final double scaleUp = 1.7159;
    public static final double scaleDown = 0.66666667;

    static double SIGMOID(double x) {
        return scaleUp * Math.tanh(scaleDown * x);
    }

    static double DSIGMOID(double S) {
        return scaleDown / scaleUp * (scaleUp + (S)) * (scaleUp - (S));
    }

    String name;
    NNLayer[] layers;
    NNLayer input;
    NNLayer output;
    double learningRate = 0.001;// 0.3;//0.0001;
    double learningRateDecrease = 1;
    double[] desiredOutput = new double[10];

    public int roundsRemaining = 500;
    public double errorRate = 0;

    public NeuralNetwork() {
        NNStructure structure = new LeCun5();
        name = structure.getName();
        layers = structure.buildNetwork();
        initNetwork();
    }

    public NeuralNetwork(File file) throws Exception {
        if (!file.exists()) {
            NNStructure structure = new LeCun5();
            name = structure.getName();
            layers = structure.buildNetwork();
        } else {
            load(new BufferedReader(new FileReader(file)));
        }
        initNetwork();
    }

    public void load(BufferedReader br) throws Exception {
        layers = new NNLayer[Integer.parseInt(br.readLine())];
        for (int layerId = 0; layerId < layers.length; layerId++) {
            String[] ss = br.readLine().split(" ");
            NNLayer layer = new NNLayer();
            layers[layerId] = layer;
            if (!"layer".equals(ss[0]))
                throw new Error();
            if (Integer.parseInt(ss[1]) != layerId)
                throw new Error();
            layer.ns = new Neuron[Integer.parseInt(ss[2])];
            layer.createWeights(Integer.parseInt(ss[3]));
            for (int i = 0; i < layer.ns.length; i++) {
                ss = br.readLine().split(" ");
                Neuron nn = new Neuron();
                nn.id = Integer.parseInt(ss[0]);
                nn.biasIndex = Integer.parseInt(ss[1]);
                nn.createConnections(Integer.parseInt(ss[2]));
                if (nn.id != i)
                    throw new Error();
                layer.ns[i] = nn;
                for (int j = 0; j < nn.fromIndex.length; j++) {
                    String line = br.readLine();
                    if (!line.startsWith("\t"))
                        throw new Error();
                    ss = line.trim().split(" ");
                    nn.fromIndex[j] = Integer.parseInt(ss[0]);
                    nn.weightIndex[j] = Integer.parseInt(ss[1]);
                }
            }
            for (int j = 0; j < layer.weights.length; j++) {
                String line = br.readLine();
                if (!line.startsWith("\t"))
                    throw new Error();
                layer.weights[j] = Double.parseDouble(line.trim());
            }
        }
        br.close();
    }

    public void save(File file) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1000000);
        PrintStream ps = new PrintStream(out);
        ps.println(layers.length);
        for (int layerId = 0; layerId < layers.length; layerId++) {
            ps.println("layer " + layerId + " " + layers[layerId].ns.length
                    + " " + layers[layerId].weights.length);
            for (Neuron n : layers[layerId].ns) {
                if (n.fromIndex.length == 0)
                    ps.println(n.id + " 0 0");
                else {
                    ps.println(n.id + " " + n.biasIndex + " "
                            + n.fromIndex.length);
                }
                for (int i = 0; i < n.fromIndex.length; i++) {
                    ps.println("\t" + n.fromIndex[i] + " " + n.weightIndex[i]);
                }
            }
            for (double w : layers[layerId].weights) {
                ps.println("\t" + w);
            }
        }
        ps.close();
        Rt.write(file, out.toByteArray());
        Rt.np("saved");
    }

    public void initNetwork() {
        for (NNLayer layer : layers)
            Arrays.fill(layer.weightCounts, 0);
        for (NNLayer layer : layers) {
            for (Neuron n : layer.ns) {
                if (n.weightIndex.length > 0)
                    layer.weightCounts[n.biasIndex]++;
                for (int i = 0; i < n.weightIndex.length; i++)
                    layer.weightCounts[n.weightIndex[i]]++;
            }
        }
        input = layers[0];
        output = layers[layers.length - 1];
        int totalConnections = 0;
        int totalNeurons = 0;
        int totalWeight = 0;
        for (NNLayer layer : layers) {
            totalNeurons += layer.ns.length;
            totalWeight += layer.weights.length;
            int t2 = 0;
            for (Neuron n : layer.ns) {
                totalConnections += n.fromIndex.length;
                t2 += n.fromIndex.length;
            }
            Rt.p("layer n=%d w=%d c=%,d", layer.ns.length,
                    layer.weights.length, t2);
        }
        Rt.p("totalConnections=%,d", totalConnections);
        Rt.p("totalNeurons=%,d", totalNeurons);
        Rt.p("totalWeight=" + totalWeight);
    }

    public boolean train(NNResult result, byte[][] image, int label) {
        calculate(result, image);
        int result2 = result(result);
        backpropagate(result, label);
        return result2 == label;
    }

    public void backpropagate(NNResult result, int label) {
        Arrays.fill(desiredOutput, -1);
        desiredOutput[label] = 1;
        double[] outputLast = result.output[result.output.length - 1];
        double[] errorLast = result.error[result.error.length - 1];
        for (int i = 0; i < outputLast.length; i++)
            errorLast[i] = outputLast[i] - desiredOutput[i];
        backpropagate(result);
    }

    public void backpropagate(NNResult result) {
        for (int layerId = layers.length - 1; layerId > 0; layerId--) {
            NNLayer layer = layers[layerId];
            double[] wn = result.wns[layerId];
            Arrays.fill(wn, 0);
            // for (NNWeight w : layer.ws)
            // w.wn = 0;
            // for (Neuron n : layers[i - 1].ns)
            // n.error = 0;
            double[] error = result.error[layerId];
            double[] error1 = result.error[layerId - 1];
            double[] output = result.output[layerId];
            double[] output1 = result.output[layerId - 1];
            Arrays.fill(error1, 0);
            for (Neuron n : layer.ns) {
                double a = error[n.id] * DSIGMOID(output[n.id]);
                // double a = n.error * DSIGMOID(n.output);
                wn[n.biasIndex] += a;
                // n.bias.wn += a;
                for (int i = 0; i < n.fromIndex.length; i++) {
                    // for (NNConnection c : n.cs) {
                    // NNWeight w = c.weight;
                    // Neuron from = c.from;
                    int from = n.fromIndex[i];
                    wn[n.weightIndex[i]] += a * output1[from];
                    // wn[c.weightIndex] += a * from.output;
                    // w.wn += a * from.output;
                    error1[from] += a * layer.weights[n.weightIndex[i]];
                    // from.error += a * w.weight;
                }
            }
            for (int i = 0; i < wn.length; i++) {
                double epsilon = learningRate / (layer.diagHessians[i] + 0.1);
                double t = epsilon * wn[i] / layer.weightCounts[i];
                layer.weights[i] -= t;
            }
        }
    }

    public void backpropagateSecondDervatives(NNResult result) {
        double[] errorLast = result.error[result.error.length - 1];
        for (int i = 0; i < output.ns.length; i++)
            errorLast[i] = 1;
        // output.ns[i].error = 1;
        for (int layerId = layers.length - 1; layerId > 0; layerId--) {
            NNLayer layer = layers[layerId];
            double[] wn = result.wns[layerId];
            Arrays.fill(wn, 0);
            // for (NNWeight w : layer.ws)
            // w.wn = 0;
            // for (Neuron n : layers[i - 1].ns)
            // n.error = 0;
            double[] error = result.error[layerId];
            double[] error1 = result.error[layerId - 1];
            double[] output = result.output[layerId];
            double[] output1 = result.output[layerId - 1];
            Arrays.fill(error1, 0);
            for (Neuron n : layer.ns) {
                double t = DSIGMOID(output[n.id]);
                double a = error[n.id] * t * t;
                wn[n.biasIndex] += a;
                // n.bias.wn += a;
                for (int i = 0; i < n.fromIndex.length; i++) {
                    int f = n.fromIndex[i];
                    int w = n.weightIndex[i];
                    t = output1[f];
                    wn[w] += a * t * t;
                    // c.weight.wn += a * c.from.output * c.from.output;
                    t = layer.weights[w];
                    error1[f] += a * t * t;
                }
            }

            double[] ds = layers[layerId].diagHessians;
            for (int i = 0; i < wn.length; i++) {
                ds[i] += wn[i] / layer.weightCounts[i];
            }
        }
    }

    public void calculate(NNResult result, byte[][] image) {
        for (int i = 0; i < layers.length; i++) {
            Arrays.fill(result.output[i], Double.POSITIVE_INFINITY);
        }
        for (int i = 0; i < input.ns.length; i++)
            result.output[0][i] = 1;
        for (int y = 0; y < 28; y++) {
            for (int x = 0; x < 28; x++) {
                result.output[0][1 + x + 29 * (y + 1)] = (255 - image[y][x] & 0xFF) / 128.0 - 1.0;
            }
        }
        forward(result);
    }

    public void forward(NNResult result) {
        for (int layerId = 1; layerId < layers.length; layerId++) {
            NNLayer layer = layers[layerId];
            double[] output = result.output[layerId];
            double[] output1 = result.output[layerId - 1];
            for (Neuron n : layer.ns) {
                double sum = layer.weights[n.biasIndex];
                for (int i = 0; i < n.fromIndex.length; i++) {
                    sum += output1[n.fromIndex[i]]
                            * layer.weights[n.weightIndex[i]];
                }
                output[n.id] = SIGMOID(sum);
            }
        }
    }

    public double loss(NNResult result, int label) {
        Arrays.fill(desiredOutput, -1);
        desiredOutput[label] = 1;
        double[] outputLast = result.output[result.output.length - 1];
        double loss = 0;
        for (int i = 0; i < output.ns.length; i++) {
            loss += Math.abs(desiredOutput[i] - outputLast[i]);
        }
        return loss;
    }

    public int result(NNResult result) {
        double max = Double.NEGATIVE_INFINITY;
        int maxIndex = 0;
        double[] outputLast = result.output[result.output.length - 1];
        for (int i = 0; i < output.ns.length; i++) {
            if (outputLast[i] > max) {
                maxIndex = i;
                max = outputLast[i];
            }
        }
        return maxIndex;
    }

    public int classify(NNResult result, byte[][] image) {
        calculate(result, image);
        return result(result);
    }

    public void merge(NeuralNetwork m) {
        for (int layerId = layers.length - 1; layerId > 0; layerId--) {
            double[] ds1 = layers[layerId].weights;
            double[] ds2 = m.layers[layerId].weights;
            for (int i = 0; i < ds1.length; i++)
                ds1[i] = (ds1[i] + ds2[i]) / 2;
        }
    }
}
