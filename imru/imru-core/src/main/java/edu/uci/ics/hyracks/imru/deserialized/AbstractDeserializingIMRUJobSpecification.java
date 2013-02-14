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

package edu.uci.ics.hyracks.imru.deserialized;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.api.IMapFunction;
import edu.uci.ics.hyracks.imru.api.IMapFunction2;
import edu.uci.ics.hyracks.imru.api.IMapFunctionFactory;
import edu.uci.ics.hyracks.imru.api.IReassemblingReduceFunction;
import edu.uci.ics.hyracks.imru.api.IReassemblingUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IReduceFunction;
import edu.uci.ics.hyracks.imru.api.IReduceFunctionFactory;
import edu.uci.ics.hyracks.imru.api.IUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IUpdateFunctionFactory;

/**
 * Base class for implementing IMRU jobs that use Java objects to
 * represent Map and Reduce outputs.
 *
 * @param <Model>
 *            The class used to represent the global model that is
 *            persisted between iterations.
 * @param <T>
 *            The class of the Map and Reduce outputs; must be
 *            serializable.
 * @author Josh Rosen
 */
public abstract class AbstractDeserializingIMRUJobSpecification<Model extends Serializable, T extends Serializable>
        implements IIMRUJobSpecification<Model>, IDeserializingIMRUJobSpecification<T, Model> {

    private static final long serialVersionUID = 1L;
    private static final int BYTES_IN_INT = 4;



    @Override
    public final IMapFunctionFactory<Model> getMapFunctionFactory() {
        return new IMapFunctionFactory<Model>() {
            @Override
            public boolean useAPI2() {
                return false;
            }

            public IMapFunction2 createMapFunction2(IMRUContext ctx,
                    int cachedDataFrameSize, Model model) {
                return null;
            };

            @Override
            public IMapFunction createMapFunction(final IMRUContext ctx, final int cachedDataFrameSize,
                    final Model model) {
                return new IMapFunction() {
                    
                    private IFrameWriter writer;
                    private IDeserializedMapFunction<T> mapFunction = getDeserializedMapFunctionFactory()
                            .createMapFunction(model, cachedDataFrameSize);

                    @Override
                    public void setFrameWriter(IFrameWriter writer) {
                        this.writer = writer;
                    }

                    @Override
                    public void open() throws HyracksDataException {
                        mapFunction.open();
                    }

                    @Override
                    public void map(ByteBuffer inputData) throws HyracksDataException {
                        mapFunction.map(inputData);
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        T object = mapFunction.close();
                        serializeToFrames(ctx, writer, object);
                    }
                };
            }
        };
    }

    @Override
    public final IReduceFunctionFactory getReduceFunctionFactory() {
        return new IReduceFunctionFactory() {
            @Override
            public IReduceFunction createReduceFunction(final IMRUReduceContext ctx) {
                return new IReassemblingReduceFunction() {

                    private IFrameWriter writer;
                    private IDeserializedReduceFunction<T> reduceFunction = getDeserializedReduceFunctionFactory()
                            .createReduceFunction();

                    @Override
                    public void setFrameWriter(IFrameWriter writer) {
                        this.writer = writer;
                    }

                    @Override
                    public void open() throws HyracksDataException {
                        reduceFunction.open();
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        T object = reduceFunction.close();
                        serializeToFrames(ctx, writer, object);
                    }

                    @Override
                    public void reduce(List<ByteBuffer> chunks) throws HyracksDataException {
                        T object = deserializeFromChunks(ctx, chunks);
                        reduceFunction.reduce(object);
                    }
                };
            }
        };
    }

    @Override
    public final IUpdateFunctionFactory<Model> getUpdateFunctionFactory() {
        return new IUpdateFunctionFactory<Model>() {

            @Override
            public IUpdateFunction createUpdateFunction(final IMRUContext ctx, final Model model) {
                return new IReassemblingUpdateFunction() {

                    private IDeserializedUpdateFunction<T> updateFunction = getDeserializedUpdateFunctionFactory()
                            .createUpdateFunction(model);

                    @Override
                    public void open() throws HyracksDataException {
                        updateFunction.open();
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        updateFunction.close();

                    }

                    @Override
                    public void update(List<ByteBuffer> chunks) throws HyracksDataException {
                        T object = deserializeFromChunks(ctx, chunks);
                        updateFunction.update(object);
                    }
                    
                     @Override
                    public Object getUpdateModel() {
                        return model;
                    }
                };
            }
        };
    }

    @SuppressWarnings("unchecked")
    private T deserializeFromChunks(IMRUContext ctx, List<ByteBuffer> chunks) throws HyracksDataException {
        int size = chunks.get(0).getInt(0);
        byte objectData[] = new byte[size];
        ByteBuffer objectDataByteBuffer = ByteBuffer.wrap(objectData);
        int remaining = size;
        // Handle the first chunk separately, since it contains the object size.
        int length = Math.min(chunks.get(0).array().length - BYTES_IN_INT, remaining);
        objectDataByteBuffer.put(chunks.get(0).array(), BYTES_IN_INT, length);
        remaining -= length;
        // Handle the remaining chunks:
        for (int i = 1; i < chunks.size(); i++) {
            length = Math.min(chunks.get(i).array().length, remaining);
            objectDataByteBuffer.put(chunks.get(i).array(), 0, length);
            remaining -= length;
        }
        // Deserialize the object from its chunks.
        // Small hack to deserialize objects using the application's class loader:
        NCApplicationContext appContext =  (NCApplicationContext) ctx.getJobletContext().getApplicationContext();
        try {
            return (T) appContext.deserialize(objectData);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void serializeToFrames(IMRUContext ctx, IFrameWriter writer, T object) throws HyracksDataException {
        byte[] objectData;
        try {
            objectData = JavaSerializationUtils.serialize(object);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        ByteBuffer frame = ctx.allocateFrame();
        int position = 0;
        frame.position(0);
        while (position < objectData.length) {
            int length = Math.min(objectData.length - position, ctx.getFrameSize());
            if (position == 0) {
                // The first chunk is a special case, since it begins
                // with an integer containing the length of the
                // serialized object.
                length = Math.min(ctx.getFrameSize() - BYTES_IN_INT, length);
                frame.putInt(objectData.length);
            }
            frame.put(objectData, position, length);
            FrameUtils.flushFrame(frame, writer);
            position += length;
        }
    }
}
