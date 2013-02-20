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
import java.util.Arrays;

public class NNLayer implements Serializable {
	public Neuron[] ns;
	public double[] weights;
	public double[] diagHessians;
	public int[] weightCounts;

	public void createWeights(int n) {
		weights = new double[n];
		diagHessians = new double[n];
		Arrays.fill(diagHessians, 1);
		weightCounts = new int[n];
	}
}
