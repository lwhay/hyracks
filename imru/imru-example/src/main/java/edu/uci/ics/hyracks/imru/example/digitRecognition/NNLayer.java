package edu.uci.ics.hyracks.imru.example.digitRecognition;

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
