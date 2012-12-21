package edu.uci.ics.hyracks.imru.example.bgd.deserialized;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedUpdateFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedUpdateFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearModel;
import edu.uci.ics.hyracks.imru.example.bgd.deserialized.DeserializedBGDJobSpecification.LossGradient;

public class DeserializedBGDUpdateFunctionFactory implements IDeserializedUpdateFunctionFactory<LossGradient, LinearModel> {

    @Override
    public IDeserializedUpdateFunction<LossGradient> createUpdateFunction(final LinearModel model) {
        return new IDeserializedUpdateFunction<DeserializedBGDJobSpecification.LossGradient>() {

            private LossGradient lossGradient;

            @Override
            public void open() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
                // Update loss
                model.loss = lossGradient.loss;
                model.loss += model.regularizationConstant * norm(model.weights.array);
                // Update weights
                for (int i = 0; i < model.weights.length; i++) {
                    model.weights.array[i] = (model.weights.array[i] - lossGradient.gradient[i] * model.stepSize)
                            * (1.0f - model.stepSize * model.regularizationConstant);
                }
                model.stepSize *= 0.9;
                model.roundsRemaining--;
            }

            @Override
            public void update(LossGradient input) throws HyracksDataException {
                if (lossGradient == null) {
                    lossGradient = input;
                } else {
                    lossGradient.loss += input.loss;
                    for (int i = 0; i < lossGradient.gradient.length; i++) {
                        lossGradient.gradient[i] += input.gradient[i];
                    }
                }
            }
        };
    }

    /**
     * @return The Euclidean norm of the vector.
     */
    public static double norm(float[] vec) {
        double norm = 0.0;
        for (double comp : vec) {
            norm += comp * comp;
        }
        return Math.sqrt(norm);
    }

}
