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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedMapFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedMapFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearExample;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearModel;
import edu.uci.ics.hyracks.imru.example.bgd.data.RecordDescriptorUtils;
import edu.uci.ics.hyracks.imru.example.bgd.deserialized.DeserializedBGDJobSpecification.LossGradient;

public class DeserializedBGDMapFunctionFactory implements IDeserializedMapFunctionFactory<LossGradient, LinearModel>{

    @Override
    public IDeserializedMapFunction<LossGradient> createMapFunction(final LinearModel model, final int cachedDataFrameSize) {
        return new IDeserializedMapFunction<LossGradient>() {

            private LossGradient lossGradient;
            private IFrameTupleAccessor accessor;
            private LinearExample example;

            @Override
            public void open() throws HyracksDataException {
                lossGradient = new LossGradient();
                lossGradient.loss = 0.0f;
                lossGradient.gradient = new float[model.numFeatures];
                accessor = new FrameTupleAccessor(cachedDataFrameSize, RecordDescriptorUtils.getDummyRecordDescriptor(2));
                example = new LinearExample();
            }

            @Override
            public LossGradient close() throws HyracksDataException {
                return lossGradient;
            }

            @Override
            public void map(ByteBuffer input) throws HyracksDataException {
                accessor.reset(input);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    example.reset(accessor, i);
                    float innerProduct = example.dot(model.weights);
                    float diff = (example.getLabel() - innerProduct);
                    lossGradient.loss += diff * diff; // Use L2 loss function.
                    example.computeGradient(model.weights, innerProduct, lossGradient.gradient);
                }
            }

        };
    }

}
