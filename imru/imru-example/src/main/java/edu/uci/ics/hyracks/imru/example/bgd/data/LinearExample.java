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

package edu.uci.ics.hyracks.imru.example.bgd.data;

import java.io.DataInputStream;
import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class LinearExample {

    private final ByteBufferInputStream bbis;
    private final DataInputStream di;

    private IFrameTupleAccessor accessor;
    private int tIndex;
    private boolean readLabel;
    private int label;

    public LinearExample() {
        bbis = new ByteBufferInputStream();
        di = new DataInputStream(bbis);
    }

    public int getLabel() throws HyracksDataException {
        if (!readLabel) {
            int tupleOffset = accessor.getTupleStartOffset(tIndex);
            int fieldStart = accessor.getFieldStartOffset(tIndex, 0);
            int startOffset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
            bbis.setByteBuffer(accessor.getBuffer(), startOffset);
            label = IntegerSerializerDeserializer.INSTANCE.deserialize(di);
            readLabel = true;
        }
        return label;
    }

    public float dot(FragmentableFloatArray weights) throws HyracksDataException {
        assert weights.fragmentStart == 0;
        int tupleOffset = accessor.getTupleStartOffset(tIndex);
        int fieldStart = accessor.getFieldStartOffset(tIndex, 1);
        int startOffset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
        bbis.setByteBuffer(accessor.getBuffer(), startOffset);
        float innerProduct = 0.0f;
        try {
            int index = di.readInt();
            while (index != -1) {
                float value = di.readFloat();
                innerProduct += value * weights.array[index - 1];
                index = di.readInt();
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return innerProduct;
    }

    public void computeGradient(FragmentableFloatArray weights, float innerProduct, float[] gradientAcc)
            throws HyracksDataException {
        assert weights.fragmentStart == 0;
        int tupleOffset = accessor.getTupleStartOffset(tIndex);
        int fieldStart = accessor.getFieldStartOffset(tIndex, 1);
        int startOffset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
        bbis.setByteBuffer(accessor.getBuffer(), startOffset);
        try {
            int index = di.readInt();
            while (index != -1) {
                float value = di.readFloat();
                gradientAcc[index - 1] += 2 * (getLabel() - innerProduct) * value;
                index = di.readInt();
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public void reset(IFrameTupleAccessor accessor, int tIndex) {
        this.accessor = accessor;
        this.tIndex = tIndex;
        readLabel = false;
    }

}
