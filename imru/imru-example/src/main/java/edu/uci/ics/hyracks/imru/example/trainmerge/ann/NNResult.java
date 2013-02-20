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

import java.io.Serializable;

public class NNResult implements Serializable {
    double[][] output;
    double[][] error;
    double[][] wns;

    public NNResult(NNLayer[] layers) {
        output = new double[layers.length][];
        error = new double[layers.length][];
        wns = new double[layers.length][];
        for (int i = 0; i < layers.length; i++) {
            output[i] = new double[layers[i].ns.length];
            error[i] = new double[layers[i].ns.length];
            wns[i] = new double[layers[i].weights.length];
        }
    }

    public double sum() {
        double sum = 0;
        for (int i = 0; i < wns.length; i++) {
            for (int j = 0; j < wns[i].length; j++) {
                sum += wns[i][j];
            }
        }
        return sum;
    }
}
