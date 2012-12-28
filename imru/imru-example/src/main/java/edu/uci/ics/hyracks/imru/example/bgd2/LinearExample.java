package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.IOException;

import edu.uci.ics.hyracks.imru.api2.TupleReader;

public class LinearExample {
    TupleReader input;
    private boolean readLabel;
    private int label;

    public LinearExample(TupleReader input) {
        this.input = input;
    }

    public int getLabel() throws IOException {
        if (!readLabel) {
            input.seekToField(0);
            label = input.readInt();
            readLabel = true;
        }
        return label;
    }

    public float dot(FragmentableFloatArray weights) throws IOException {
        assert weights.fragmentStart == 0;
        input.seekToField(1);
        float innerProduct = 0.0f;
        while (true) {
            int index = input.readInt();
            if (index < 0)
                break;
            float value = input.readFloat();
            innerProduct += value * weights.array[index - 1];
        }
        return innerProduct;
    }

    public void computeGradient(FragmentableFloatArray weights,
            float innerProduct, float[] gradientAcc) throws IOException {
        assert weights.fragmentStart == 0;
        input.seekToField(1);
        while (true) {
            int index = input.readInt();
            if (index < 0)
                break;
            float value = input.readFloat();
            gradientAcc[index - 1] += 2 * (getLabel() - innerProduct) * value;
        }
    }
}
