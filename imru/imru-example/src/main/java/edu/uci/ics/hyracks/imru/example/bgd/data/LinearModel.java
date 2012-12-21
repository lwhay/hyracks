package edu.uci.ics.hyracks.imru.example.bgd.data;

import edu.uci.ics.hyracks.imru.api.IModel;

public class LinearModel implements IModel {

    private static final long serialVersionUID = 1L;
    public final int numFeatures;
    public FragmentableFloatArray weights;
    public float loss;
    public float stepSize = 1.0f;
    public float regularizationConstant = 0.5f;
    public int roundsRemaining;


    public LinearModel(int numFeatures, int numRounds) {
        this.numFeatures = numFeatures;
        this.weights = new FragmentableFloatArray(new float[numFeatures]);
        this.roundsRemaining = numRounds;
    }

}