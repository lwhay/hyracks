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

package edu.uci.ics.hyracks.imru.example.bgd;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameConstants;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class FragmentableFloatArray implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int BYTES_IN_FLOAT = 4;
    private static final int BYTES_IN_INT = 4;

    public final int length;
    public final int fragmentStart;
    public final float[] array;

    public FragmentableFloatArray(float[] wholeArray) {
        this(wholeArray, 0);
    }

    public FragmentableFloatArray(float[] fragmentArray, int fragmentStart) {
        length = fragmentArray.length;
        this.fragmentStart = fragmentStart;
        array = fragmentArray;
    }

    public void writeFragments(IFrameWriter writer, IHyracksTaskContext ctx) throws HyracksDataException {
        ArrayTupleBuilder gradientBuilder = new ArrayTupleBuilder(2);
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ByteBuffer outFrame = ctx.allocateFrame();
        appender.reset(outFrame, true);

        // Calculate how large each fragment should be based on the
        // frame size. This is a rough approximation.
        int fieldSlotsLength = 2 * 4;
        int maxFragmentLength = (int) Math.round(Math.floor(ctx.getFrameSize() - FrameConstants.SIZE_LEN
                - fieldSlotsLength - 3 * BYTES_IN_INT)
                / (BYTES_IN_FLOAT));

        // Output the fragments
        int fragmentLength;
        int numFragments = 0;
        for (int offset = 0; offset < array.length; offset += fragmentLength) {
            fragmentLength = Math.min(maxFragmentLength, array.length - offset);
            float[] fragment = new float[fragmentLength];
            System.arraycopy(array, offset, fragment, 0, fragment.length);
            gradientBuilder.reset();
            gradientBuilder.addField(IntegerSerializerDeserializer.INSTANCE, offset);
            gradientBuilder.addField(FloatArraySerializerDeserializer.INSTANCE, fragment);
            if (!appender.append(gradientBuilder.getFieldEndOffsets(), gradientBuilder.getByteArray(), 0,
                    gradientBuilder.getSize())) {
                FrameUtils.flushFrame(outFrame, writer);
                appender.reset(outFrame, true);
                if (!appender.append(gradientBuilder.getFieldEndOffsets(), gradientBuilder.getByteArray(), 0,
                        gradientBuilder.getSize())) {
                    throw new HyracksDataException("Couldn't append to array fragment frame: size is " + gradientBuilder.getSize());
                }
            }
            numFragments++;
        }
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outFrame, writer);
        }
        /* LOG.info("Split gradient with " + featureLength + " features into " + numFragments
                + " fragments, each containing at most " + maxFragmentLength + " features"); */
    }

    public static FragmentableFloatArray readFromFrame(IFrameTupleAccessor accessor, int tIndex)
            throws HyracksDataException {
        // Fragment length
        int tupleOffset = accessor.getTupleStartOffset(tIndex);
        int fieldStart = accessor.getFieldStartOffset(tIndex, 0);
        int startOffset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream di = new DataInputStream(bbis);
        bbis.setByteBuffer(accessor.getBuffer(), startOffset);
        int fragmentStart = IntegerSerializerDeserializer.INSTANCE.deserialize(di);

        // Array fragment
        fieldStart = accessor.getFieldStartOffset(tIndex, 1);
        startOffset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
        bbis.setByteBuffer(accessor.getBuffer(), startOffset);
        float[] gradient = FloatArraySerializerDeserializer.INSTANCE.deserialize(di);

        try {
            di.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return new FragmentableFloatArray(gradient, fragmentStart);
    }

    public void accumulate(FragmentableFloatArray other) {
        for (int i = 0; i < other.length; i++) {
            this.array[i + other.fragmentStart] += other.array[i];
        }
    }

}
