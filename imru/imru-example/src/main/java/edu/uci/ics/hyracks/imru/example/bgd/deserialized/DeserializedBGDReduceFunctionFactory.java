package edu.uci.ics.hyracks.imru.example.bgd.deserialized;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedReduceFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedReduceFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.deserialized.DeserializedBGDJobSpecification.LossGradient;

public class DeserializedBGDReduceFunctionFactory implements IDeserializedReduceFunctionFactory<LossGradient> {

    @Override
    public IDeserializedReduceFunction<LossGradient> createReduceFunction() {
        return new IDeserializedReduceFunction<DeserializedBGDJobSpecification.LossGradient>() {

            private LossGradient lossGradient;

            @Override
            public void open() throws HyracksDataException {
            }

            @Override
            public LossGradient close() throws HyracksDataException {
                return lossGradient;
            }

            @Override
            public void reduce(LossGradient input) throws HyracksDataException {
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
}
