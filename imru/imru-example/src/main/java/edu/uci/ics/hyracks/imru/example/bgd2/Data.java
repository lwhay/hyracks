package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.IOException;
import java.io.Serializable;

public class Data implements Serializable {
    public int label;
    public int[] fieldIds;
    public float[] values;

    public float dot(FragmentableFloatArray weights) throws IOException {
        assert weights.fragmentStart == 0;
        float innerProduct = 0.0f;
        for (int i = 0; i < fieldIds.length; i++)
            innerProduct += values[i] * weights.array[fieldIds[i] - 1];
        return innerProduct;
    }

    public void computeGradient(FragmentableFloatArray weights, float innerProduct, float[] gradientAcc)
            throws IOException {
        assert weights.fragmentStart == 0;
        for (int i = 0; i < fieldIds.length; i++)
            gradientAcc[fieldIds[i] - 1] += 2 * (label - innerProduct) * values[i];
    }
}
