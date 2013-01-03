package edu.uci.ics.hyracks.imru.example.digitRecognition;

import java.io.Serializable;

public class Neuron implements Serializable {
	public int id;
	public int biasIndex;
	public int[] fromIndex;
	public int[] weightIndex;
	public void createConnections(int n) {
		fromIndex = new int[n];
		weightIndex = new int[n];
	}
}
