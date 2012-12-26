package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.Serializable;

public class LossGradient implements Serializable {
    float loss;
    float[] gradient;

    public LossGradient(int numFeatures) {
        loss = 0;
        gradient = new float[numFeatures];
    }
}