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

import java.util.Random;

public class LeCun5 extends NNStructure {
    @Override
    public String getName() {
        return "LeCun5";
    }

    public NNLayer[] buildNetwork()  {
        NNLayer[] layers = new NNLayer[5];
        for (int i = 0; i < 5; i++) {
            layers[i] = new NNLayer();
        }
        layers[0].ns = new Neuron[29 * 29];
        layers[0].createWeights(0);
        for (int i = 0; i < layers[0].ns.length; i++) {
            Neuron n = layers[0].ns[i] = new Neuron();
            n.id = i;
            n.createConnections(0);
        }
        buildConvLayer(layers[0], layers[1], 29, 1, 13, 5, 6);
        buildConvLayer(layers[1], layers[2], 13, 6, 5, 5, 50);
        buildFullConnectedLayer(layers[2], layers[3], 5 * 5 * 50, 100);
        buildFullConnectedLayer(layers[3], layers[4], 100, 10);
        Random random = new Random();
        for (NNLayer layer : layers)
            for (int i = 0; i < layer.weights.length; i++)
                layer.weights[i] = random.nextDouble() * 0.02 - 0.01;
        return layers;
    }
}
