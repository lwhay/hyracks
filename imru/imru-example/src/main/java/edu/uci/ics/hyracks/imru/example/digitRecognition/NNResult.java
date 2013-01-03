package edu.uci.ics.hyracks.imru.example.digitRecognition;

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
