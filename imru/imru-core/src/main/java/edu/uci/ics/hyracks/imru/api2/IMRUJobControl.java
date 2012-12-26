package edu.uci.ics.hyracks.imru.api2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
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
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.deserialized.AbstractDeserializingIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedMapFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedMapFunctionFactory;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedReduceFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedReduceFunctionFactory;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedUpdateFunction;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedUpdateFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.R;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.GenericAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NAryAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NoAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;

public class IMRUJobControl<Model extends IModel, T extends Serializable> {
    public HyracksConnection hcc;
    public Configuration conf = new Configuration();
    public ConfigurationFactory confFactory;
    IJobFactory jobFactory;
    private static final int BYTES_IN_INT = 4;
    private static ExecutorService threadPool = Executors.newCachedThreadPool();

    public void connect(String ccHost, int ccPort, String hadoopConfPath,
            String clusterConfPath) throws Exception {
        hcc = new HyracksConnection(ccHost, ccPort);

        if (!new File(hadoopConfPath).exists()) {
            System.err.println("Hadoop conf path does not exist!");
            System.exit(-1);
        }
        // Hadoop configuration
        conf.addResource(new Path(hadoopConfPath + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/mapred-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));
        ClusterConfig.setConfPath(clusterConfPath);
        confFactory = new ConfigurationFactory(conf);
    }

    public void selectNoAggregation(String examplePaths) {
        jobFactory = new NoAggregationIMRUJobFactory(examplePaths, confFactory);
    }

    public void selectGenericAggregation(String examplePaths, int aggCount) {
        if (aggCount < 1)
            throw new IllegalArgumentException(
                    "Must specify a nonnegative aggregator count using the -agg-count option");
        jobFactory = new GenericAggregationIMRUJobFactory(examplePaths,
                confFactory, aggCount);
    }

    public void selectNAryAggregation(String examplePaths, int fanIn) {
        if (fanIn < 1) {
            throw new IllegalArgumentException(
                    "Must specify nonnegative -fan-in");
        }
        jobFactory = new NAryAggregationIMRUJobFactory(examplePaths,
                confFactory, fanIn);
    }

    IMRUDriver<Model> driver;

    static class JobSpecificationTmp<Model extends IModel, T extends Serializable>
            extends AbstractDeserializingIMRUJobSpecification<Model, T> {
        IMRUJobTmp<Model, T> job2;

        public JobSpecificationTmp(IMRUJobTmp<Model, T> job2) {
            this.job2 = job2;
        }

        @Override
        public ITupleParserFactory getTupleParserFactory() {
            return new ITupleParserFactory() {
                @Override
                public ITupleParser createTupleParser(
                        final IHyracksTaskContext ctx) {
                    return new ITupleParser() {
                        @Override
                        public void parse(InputStream in, IFrameWriter writer)
                                throws HyracksDataException {
                            job2.parse(ctx, in, writer);
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
        public IDeserializedMapFunctionFactory<T, Model> getDeserializedMapFunctionFactory() {
            return new IDeserializedMapFunctionFactory<T, Model>() {
                @Override
                public IDeserializedMapFunction<T> createMapFunction(
                        final Model model, final int cachedDataFrameSize) {
                    return new IDeserializedMapFunction<T>() {
                        @Override
                        public void open() throws HyracksDataException {
                            job2.openMap(model, cachedDataFrameSize);
                        }

                        @Override
                        public T close() throws HyracksDataException {
                            return job2.closeMap(model, cachedDataFrameSize);
                        }

                        @Override
                        public void map(ByteBuffer input)
                                throws HyracksDataException {
                            job2.map(input, model, cachedDataFrameSize);
                        }
                    };
                }
            };
        }

        @Override
        public IDeserializedReduceFunctionFactory<T> getDeserializedReduceFunctionFactory() {
            return new IDeserializedReduceFunctionFactory<T>() {
                @Override
                public IDeserializedReduceFunction<T> createReduceFunction() {
                    return new IDeserializedReduceFunction<T>() {
                        @Override
                        public void open() throws HyracksDataException {
                            job2.openReduce();
                        }

                        @Override
                        public T close() throws HyracksDataException {
                            return job2.closeReduce();
                        }

                        @Override
                        public void reduce(T input) throws HyracksDataException {
                            job2.reduce(input);
                        }
                    };
                }
            };
        }

        @Override
        public IDeserializedUpdateFunctionFactory<T, Model> getDeserializedUpdateFunctionFactory() {
            return new IDeserializedUpdateFunctionFactory<T, Model>() {
                public IDeserializedUpdateFunction<T> createUpdateFunction(
                        final Model model) {
                    return new IDeserializedUpdateFunction<T>() {
                        @Override
                        public void open() throws HyracksDataException {
                            job2.openUpdate(model);
                        }

                        @Override
                        public void close() throws HyracksDataException {
                            job2.closeUpdate(model);
                        }

                        @Override
                        public void update(T input) throws HyracksDataException {
                            job2.update(input, model);
                        }
                    };
                }
            };
        }
    }

    public JobStatus run(IMRUJobTmp<Model, T> job2, Model initialModel,
            String tempPath, String app) throws Exception {
        JobSpecificationTmp<Model, T> job = new JobSpecificationTmp<Model, T>(
                job2);
        driver = new IMRUDriver<Model>(hcc, job, initialModel, jobFactory,
                conf, tempPath, app);
        return driver.run();
    }

    static class JobSpecification2<Model extends IModel> implements
            IIMRUJobSpecification<Model> {
        IMRUJob2<Model> job2;

        public JobSpecification2(IMRUJob2<Model> job2) {
            this.job2 = job2;
        }

        @Override
        public ITupleParserFactory getTupleParserFactory() {
            return new ITupleParserFactory() {
                @Override
                public ITupleParser createTupleParser(
                        final IHyracksTaskContext ctx) {
                    return new ITupleParser() {
                        @Override
                        public void parse(InputStream in, IFrameWriter writer)
                                throws HyracksDataException {
                            job2.parse(ctx, in, writer);
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
                                IFrameWriter writer)
                                throws HyracksDataException {
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            job2.map(input, model, out, cachedDataFrameSize);
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
                        private ASyncIO io;

                        @Override
                        public void setFrameWriter(IFrameWriter writer) {
                            this.writer = writer;
                        }

                        @Override
                        public void open() throws HyracksDataException {
                            io = new ASyncIO();
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    Iterator<byte[]> input = io.getInput();
                                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                                    try {
                                        job2.reduce(ctx, input, out);
                                        byte[] objectData = out.toByteArray();
                                        serializeToFrames(ctx, writer,
                                                objectData);
                                    } catch (HyracksDataException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }

                        @Override
                        public void close() throws HyracksDataException {
                            io.close();
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
                        private ASyncIO io;

                        @Override
                        public void open() throws HyracksDataException {
                            io = new ASyncIO();
                            threadPool.execute(new Runnable() {
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
    }

    public JobStatus run2(IMRUJob2<Model> job2, Model initialModel,
            String tempPath, String app) throws Exception {
        JobSpecification2<Model> job = new JobSpecification2<Model>(job2);
        driver = new IMRUDriver<Model>(hcc, job, initialModel, jobFactory,
                conf, tempPath, app);
        return driver.run();
    }

    static class Job<Model extends IModel, T extends Serializable> extends
            IMRUJob2<Model> {
        IMRUJob<Model, T> job;

        public Job(IMRUJob<Model, T> job) {
            this.job = job;
        }

        @Override
        public int getCachedDataFrameSize() {
            return job.getCachedDataFrameSize();
        }

        @Override
        public Model initModel() {
            return job.initModel();
        }

        @Override
        public void map(Iterator<ByteBuffer> input, Model model,
                OutputStream output, int cachedDataFrameSize)
                throws HyracksDataException {
            T object = job.map(input, model, cachedDataFrameSize);
            byte[] objectData;
            try {
                objectData = JavaSerializationUtils.serialize(object);
                output.write(objectData);
                output.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void parse(IHyracksTaskContext ctx, InputStream in,
                IFrameWriter writer) throws HyracksDataException {
            job.parse(ctx, in, writer);
        }

        @Override
        public void reduce(final IHyracksTaskContext ctx,
                final Iterator<byte[]> input, OutputStream output)
                throws HyracksDataException {
            Iterator<T> iterator = new Iterator<T>() {
                @Override
                public void remove() {
                }

                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }

                @Override
                public T next() {
                    byte[] objectData = input.next();
                    if (objectData == null)
                        return null;
                    NCApplicationContext appContext = (NCApplicationContext) ctx
                            .getJobletContext().getApplicationContext();
                    try {
                        return (T) appContext.deserialize(objectData);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
            T object = job.reduce(iterator);
            byte[] objectData;
            try {
                objectData = JavaSerializationUtils.serialize(object);
                output.write(objectData);
                output.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public boolean shouldTerminate(Model model) {
            return job.shouldTerminate(model);
        }

        @Override
        public void update(final IHyracksTaskContext ctx,
                final Iterator<byte[]> input, Model model)
                throws HyracksDataException {
            Iterator<T> iterator = new Iterator<T>() {
                @Override
                public void remove() {
                }

                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }

                @Override
                public T next() {
                    byte[] objectData = input.next();
                    if (objectData == null)
                        return null;
                    NCApplicationContext appContext = (NCApplicationContext) ctx
                            .getJobletContext().getApplicationContext();
                    try {
                        return (T) appContext.deserialize(objectData);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
            job.update(iterator, model);
        }
    }

    public JobStatus run(final IMRUJob<Model, T> job, String tempPath,
            String app) throws Exception {
        Model initialModel = job.initModel();
        JobSpecification2<Model> job2 = new JobSpecification2<Model>(
                new Job<Model, T>(job));
        driver = new IMRUDriver<Model>(hcc, job2, initialModel, jobFactory,
                conf, tempPath, app);
        return driver.run();
    }

    /**
     * @return The number of iterations performed.
     */
    public int getIterationCount() {
        return driver.getIterationCount();
    }

    /**
     * @return The most recent global model.
     */
    public Model getModel() {
        return driver.getModel();
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
