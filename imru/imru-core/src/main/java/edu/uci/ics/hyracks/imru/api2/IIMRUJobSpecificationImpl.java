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

public class IIMRUJobSpecificationImpl<Model extends IModel> implements
        IIMRUJobSpecification<Model> {
    private static final int BYTES_IN_INT = 4;
    private static ExecutorService threadPool = Executors.newCachedThreadPool();
    IMRUJob2<Model> job2;

    public IIMRUJobSpecificationImpl(IMRUJob2<Model> job2) {
        this.job2 = job2;
    }

    @Override
    public ITupleParserFactory getTupleParserFactory() {
        return new ITupleParserFactory() {
            @Override
            public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
                return new ITupleParser() {
                    @Override
                    public void parse(InputStream in, IFrameWriter writer)
                            throws HyracksDataException {
                        try {
                            job2.parse(ctx, in, writer);
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

            public IMapFunction2 createMapFunction2(
                    final IHyracksTaskContext ctx,
                    final int cachedDataFrameSize, final Model model) {
                return new IMapFunction2() {
                    @Override
                    public void map(Iterator<ByteBuffer> input,
                            IFrameWriter writer) throws HyracksDataException {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        job2.map(ctx,input, model, out, cachedDataFrameSize);
                        byte[] objectData = out.toByteArray();
                        serializeToFrames(ctx, writer, objectData);
                    }
                };
            };

            @Override
            public IMapFunction createMapFunction(
                    final IHyracksTaskContext ctx,
                    final int cachedDataFrameSize, final Model model) {
                return null;
            }
        };
    }

    @Override
    public IReduceFunctionFactory getReduceFunctionFactory() {
        return new IReduceFunctionFactory() {
            @Override
            public IReduceFunction createReduceFunction(
                    final IHyracksTaskContext ctx) {
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
                                    job2.reduce(ctx, input, out);
                                    byte[] objectData = out.toByteArray();
                                    serializeToFrames(ctx, writer, objectData);
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
                    public void reduce(List<ByteBuffer> chunks)
                            throws HyracksDataException {
                        byte[] data = deserializeFromChunks(ctx, chunks);
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
            public IUpdateFunction createUpdateFunction(
                    final IHyracksTaskContext ctx, final Model model) {
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
                                    job2.update(ctx, input, model);
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
                    public void update(List<ByteBuffer> chunks)
                            throws HyracksDataException {
                        byte[] data = deserializeFromChunks(ctx, chunks);
                        io.add(data);
                    }
                };
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static byte[] deserializeFromChunks(IHyracksTaskContext ctx,
            List<ByteBuffer> chunks) throws HyracksDataException {
        int size = chunks.get(0).getInt(0);
        byte objectData[] = new byte[size];
        ByteBuffer objectDataByteBuffer = ByteBuffer.wrap(objectData);
        int remaining = size;
        // Handle the first chunk separately, since it contains the object size.
        int length = Math.min(chunks.get(0).array().length - BYTES_IN_INT,
                remaining);
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

    private static void serializeToFrames(IHyracksTaskContext ctx,
            IFrameWriter writer, byte[] objectData) throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        int position = 0;
        frame.position(0);
        while (position < objectData.length) {
            int length = Math.min(objectData.length - position, ctx
                    .getFrameSize());
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