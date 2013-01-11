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

package edu.uci.ics.hyracks.imru.example.bgd.job;

import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.imru.api.IMapFunction;
import edu.uci.ics.hyracks.imru.api.IMapFunction2;
import edu.uci.ics.hyracks.imru.api.IMapFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.FragmentableFloatArray;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearExample;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearModel;
import edu.uci.ics.hyracks.imru.example.bgd.data.RecordDescriptorUtils;

public class BGDMapFunctionFactory implements IMapFunctionFactory<LinearModel> {
     @Override
    public IMapFunction2 createMapFunction2(IHyracksTaskContext ctx,
            int cachedDataFrameSize, LinearModel model) {
        return null;
    }
      @Override
    public boolean useAPI2() {
        return false;
    }
    @Override
    public IMapFunction createMapFunction(final IHyracksTaskContext ctx, final int cachedDataFrameSize, final LinearModel model) {
        return new IMapFunction() {

            private IFrameWriter writer;
            private FragmentableFloatArray gradient;
            private float loss;
            private IFrameTupleAccessor accessor;
            private LinearExample example;

            @Override
            public void setFrameWriter(IFrameWriter writer) {
                this.writer = writer;
                accessor = new FrameTupleAccessor(cachedDataFrameSize, RecordDescriptorUtils.getDummyRecordDescriptor(2));
                example = new LinearExample();
            }

            @Override
            public void open() throws HyracksDataException {
                float[] gradientArr = new float[model.numFeatures];
                Arrays.fill(gradientArr, 0.0f);
                gradient = new FragmentableFloatArray(gradientArr);
                loss = 0.0f;
            }

            @Override
            public void map(ByteBuffer inputData) throws HyracksDataException {
                accessor.reset(inputData);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    example.reset(accessor, i);
                    float innerProduct = example.dot(model.weights);
                    float diff = (example.getLabel() - innerProduct);
                    loss += diff * diff; // Use L2 loss function.
                    example.computeGradient(model.weights, innerProduct, gradient.array);
                }
            }

            @Override
            public void close() throws HyracksDataException {
                // Write Loss
                ArrayTupleBuilder lossBuilder = new ArrayTupleBuilder(1);
                FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                ByteBuffer outFrame = ctx.allocateFrame();
                appender.reset(outFrame, true);
                lossBuilder.reset();
                lossBuilder.addField(FloatSerializerDeserializer.INSTANCE, loss);
                System.out.println("Map: outputting loss " + loss);
                appender.append(lossBuilder.getFieldEndOffsets(), lossBuilder.getByteArray(), 0, lossBuilder.getSize());
                FrameUtils.flushFrame(outFrame, writer);
                // Write gradient
                gradient.writeFragments(writer, ctx);
            }
        };
    }

}
