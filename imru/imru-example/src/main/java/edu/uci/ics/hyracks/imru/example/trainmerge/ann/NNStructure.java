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

abstract public class NNStructure {
    abstract public String getName();

    abstract public NNLayer[] buildNetwork();

    public static void buildConvLayer(NNLayer last, NNLayer l, int prevDim,
            int prevGroupCount, int dim, int weightDim, int groupCount) {
        int weightCountPerGroup = weightDim * weightDim * prevGroupCount + 1;
        l.ns = new Neuron[dim * dim * groupCount];
        l.createWeights(weightCountPerGroup * groupCount);
        int nid = 0;
        for (int gid = 0; gid < groupCount; gid++) {
            for (int cy = 0; cy < dim; cy++) {
                for (int cx = 0; cx < dim; cx++) {
                    Neuron n = new Neuron();
                    n.id = nid;
                    l.ns[nid++] = n;
                    n.biasIndex = weightCountPerGroup * gid;
                    n.createConnections(weightDim * weightDim * prevGroupCount);
                    int pos = 0;
                    for (int y = 0; y < weightDim; y++) {
                        for (int x = 0; x < weightDim; x++) {
                            for (int pg = 0; pg < prevGroupCount; pg++) {
                                n.fromIndex[pos] = pg * prevDim * prevDim
                                        + prevDim * (y + cy * 2) + (x + cx * 2);
                                n.weightIndex[pos] = gid
                                        * (weightDim * weightDim
                                                * prevGroupCount + 1)
                                        + (y * weightDim + x) * prevGroupCount
                                        + pg + 1;
                                pos++;
                            }
                        }
                    }
                    // System.exit(0);
                }
            }
        }
    }

    public static void buildFullConnectedLayer(NNLayer last, NNLayer l,
            int prevCount, int count) {
        l.ns = new Neuron[count];
        l.createWeights((prevCount + 1) * count);
        int wid = 0;
        for (int nid = 0; nid < l.ns.length; nid++) {
            Neuron n = new Neuron();
            n.id = nid;
            l.ns[nid] = n;
            n.biasIndex = wid++;
            n.createConnections(prevCount);
            for (int i = 0; i < n.fromIndex.length; i++) {
                n.fromIndex[i] = i;
                n.weightIndex[i] = wid++;
            }
        }
    }
}
