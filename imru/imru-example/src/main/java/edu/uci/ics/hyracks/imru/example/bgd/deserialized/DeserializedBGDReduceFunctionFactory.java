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
