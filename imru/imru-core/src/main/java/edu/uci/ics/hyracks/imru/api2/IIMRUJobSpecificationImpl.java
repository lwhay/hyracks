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

package edu.uci.ics.hyracks.imru.api2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IMapFunction;
import edu.uci.ics.hyracks.imru.api.IMapFunction2;
import edu.uci.ics.hyracks.imru.api.IMapFunctionFactory;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.api.IReassemblingReduceFunction;
import edu.uci.ics.hyracks.imru.api.IReassemblingUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IReduceFunction;
import edu.uci.ics.hyracks.imru.api.IReduceFunctionFactory;
import edu.uci.ics.hyracks.imru.api.IUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IUpdateFunctionFactory;

/**
 * Implement a IMRUJobSpecification interface using
 * provided IMRUJob2 implementation.
 * 
 * @author wangrui
 * @param <Model>
 */
public class IIMRUJobSpecificationImpl<Model extends IModel> implements IIMRUJobSpecification<Model> {
    private static final int BYTES_IN_INT = 4;
    private static ExecutorService threadPool = Executors.newCachedThreadPool();
    IIMRUJob2<Model> job2;

    public IIMRUJobSpecificationImpl(IIMRUJob2<Model> job2) {
        this.job2 = job2;
    }

    @Override
    public ITupleParserFactory getTupleParserFactory() {
        return new ITupleParserFactory() {
            @Override
            public ITupleParser createTupleParser(IHyracksTaskContext ctx) {
                final IMRUContext context = new IMRUContext(ctx);
                return new ITupleParser() {
                    @Override
                    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                        try {
                            job2.parse(context, in, new FrameWriter(writer));
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public int getCachedDataFrameSize() {
        return job2.getCachedDataFrameSize();
    }

    public boolean shouldTerminate(Model model) {
        return job2.shouldTerminate(model);
    };

    @Override
    public IMapFunctionFactory<Model> getMapFunctionFactory() {
        return new IMapFunctionFactory<Model>() {
            @Override
            public boolean useAPI2() {
                return true;
            }

            public IMapFunction2 createMapFunction2(IHyracksTaskContext ctx, final int cachedDataFrameSize,
                    final Model model) {
                final IMRUContext context = new IMRUContext(ctx);
                return new IMapFunction2() {
                    @Override
                    public void map(Iterator<ByteBuffer> input, IFrameWriter writer) throws HyracksDataException {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        job2.map(context, input, model, out, cachedDataFrameSize);
                        byte[] objectData = out.toByteArray();
                        serializeToFrames(context, writer, objectData);
                    }
                };
            };

            @Override
            public IMapFunction createMapFunction(final IHyracksTaskContext ctx, final int cachedDataFrameSize,
                    final Model model) {
                return null;
            }
        };
    }

    @Override
    public IReduceFunctionFactory getReduceFunctionFactory() {
        return new IReduceFunctionFactory() {
            @Override
            public IReduceFunction createReduceFunction(IHyracksTaskContext ctx) {
                final IMRUContext context = new IMRUContext(ctx);
                return new IReassemblingReduceFunction() {
                    private IFrameWriter writer;
                    private ASyncIO<byte[]> io;
                    Future future;

                    @Override
                    public void setFrameWriter(IFrameWriter writer) {
                        this.writer = writer;
                    }

                    @Override
                    public void open() throws HyracksDataException {
                        io = new ASyncIO<byte[]>();
                        future = threadPool.submit(new Runnable() {
                            @Override
                            public void run() {
                                Iterator<byte[]> input = io.getInput();
                                ByteArrayOutputStream out = new ByteArrayOutputStream();
                                try {
                                    job2.reduce(context, input, out);
                                    byte[] objectData = out.toByteArray();
                                    serializeToFrames(context, writer, objectData);
                                } catch (HyracksDataException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        io.close();
                        try {
                            future.get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void reduce(List<ByteBuffer> chunks) throws HyracksDataException {
                        byte[] data = deserializeFromChunks(context, chunks);
                        io.add(data);
                    }
                };
            }
        };
    }

    @Override
    public IUpdateFunctionFactory<Model> getUpdateFunctionFactory() {
        return new IUpdateFunctionFactory<Model>() {

            @Override
            public IUpdateFunction createUpdateFunction(IHyracksTaskContext ctx, final Model model) {
                final IMRUContext context = new IMRUContext(ctx);
                return new IReassemblingUpdateFunction() {
                    private ASyncIO<byte[]> io;
                    Future future;

                    @Override
                    public void open() throws HyracksDataException {
                        io = new ASyncIO<byte[]>();
                        future = threadPool.submit(new Runnable() {
                            @Override
                            public void run() {
                                Iterator<byte[]> input = io.getInput();
                                try {
                                    job2.update(context, input, model);
                                } catch (HyracksDataException e) {
                                    e.printStackTrace();
                                }
                            }
                        });

                    }

                    @Override
                    public void close() throws HyracksDataException {
                        io.close();
                        try {
                            future.get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void update(List<ByteBuffer> chunks) throws HyracksDataException {
                        byte[] data = deserializeFromChunks(context, chunks);
                        io.add(data);
                    }
                };
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static byte[] deserializeFromChunks(IMRUContext ctx, List<ByteBuffer> chunks) throws HyracksDataException {
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
        return objectData;
    }

    private static void serializeToFrames(IMRUContext ctx, IFrameWriter writer, byte[] objectData)
            throws HyracksDataException {
        ByteBuffer frame = ctx.ctx.allocateFrame();
        int position = 0;
        frame.position(0);
        while (position < objectData.length) {
            int length = Math.min(objectData.length - position, ctx.ctx.getFrameSize());
            if (position == 0) {
                // The first chunk is a special case, since it begins
                // with an integer containing the length of the
                // serialized object.
                length = Math.min(ctx.ctx.getFrameSize() - BYTES_IN_INT, length);
                frame.putInt(objectData.length);
            }
            frame.put(objectData, position, length);
            FrameUtils.flushFrame(frame, writer);
            position += length;
        }
    }
}