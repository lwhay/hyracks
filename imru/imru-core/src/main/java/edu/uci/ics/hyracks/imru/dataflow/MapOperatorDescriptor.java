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

package edu.uci.ics.hyracks.imru.dataflow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.util.log.Log;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMapFunction;
import edu.uci.ics.hyracks.imru.api.IMapFunction2;
import edu.uci.ics.hyracks.imru.api.IMapFunctionFactory;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.base.IConfigurationFactory;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.RunFileContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.state.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;
import edu.uci.ics.hyracks.imru.util.MemoryStatsLogger;

/**
 * Evaluates the map function in an iterative map reduce update job.
 * 
 * @param <Model>
 *            The class used to represent the global model that is
 *            persisted between iterations.
 * @author Josh Rosen
 */
public class MapOperatorDescriptor<Model extends IModel> extends AbstractSingleActivityOperatorDescriptor {

    private static Logger LOG = Logger.getLogger(MapOperatorDescriptor.class.getName());

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[1]);

    private final IIMRUJobSpecification<Model> imruSpec;
    private final String envInPath;
    private final IConfigurationFactory confFactory;
    private final int roundNum;
    private final String name;

    /**
     * Create a new MapOperatorDescriptor.
     * 
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU job specification
     * @param envInPath
     *            The HDFS path to read the current environment from.
     * @param confFactory
     *            A Hadoop configuration, used for HDFS.
     * @param roundNum
     *            The round number.
     */
    public MapOperatorDescriptor(JobSpecification spec, IIMRUJobSpecification<Model> imruSpec, String envInPath,
            IConfigurationFactory confFactory, int roundNum, String name) {
        super(spec, 0, 1);
        recordDescriptors[0] = dummyRecordDescriptor;
        this.imruSpec = imruSpec;
        this.envInPath = envInPath;
        this.confFactory = confFactory;
        this.roundNum = roundNum;
        this.name = name;
    }

    private static class MapOperatorNodePushable<Model extends IModel> extends
            AbstractUnaryOutputSourceOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final IHyracksTaskContext fileCtx;
        private final IIMRUJobSpecification<Model> imruSpec;
        private final String envInPath;
        private final IConfigurationFactory confFactory;
        private final int partition;
        private final int roundNum;
        private final String name;

        public MapOperatorNodePushable(IHyracksTaskContext ctx, IIMRUJobSpecification<Model> imruSpec,
                String envInPath, IConfigurationFactory confFactory, int partition, int roundNum, String name) {
            this.ctx = ctx;
            this.imruSpec = imruSpec;
            this.envInPath = envInPath;
            this.confFactory = confFactory;
            this.partition = partition;
            this.roundNum = roundNum;
            this.name = name;
            fileCtx = new RunFileContext(ctx, imruSpec.getCachedDataFrameSize());
        }

        @SuppressWarnings("unchecked")
        @Override
        public void initialize() throws HyracksDataException {
            MemoryStatsLogger.logHeapStats(LOG, "MapOperator: Before reading examples");
            writer.open();

            // Load the environment and weight vector.
            // For efficiency reasons, the Environment and weight vector are
            // shared across all MapOperator partitions.
            INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
            IMRURuntimeContext context = (IMRURuntimeContext) appContext.getApplicationObject();
            Model model;
            synchronized (context.envLock) {
                if (context.modelAge < roundNum) {
                    try {
                        long start = System.currentTimeMillis();
                        InputStream fileInput;
                        if (confFactory == null) {
                            fileInput = new FileInputStream(new File(envInPath));
                        } else {
                            Configuration conf = confFactory.createConfiguration();
                            FileSystem dfs;
                            dfs = FileSystem.get(conf);
                            fileInput = dfs.open(new Path(envInPath));
                        }
                        ObjectInputStream input = new ObjectInputStream(fileInput);
                        model = (Model) input.readObject();
                        context.model = model;
                        context.modelAge = roundNum;
                        input.close();
                        long end = System.currentTimeMillis();
                        long modelReadTime = (end - start);
                        LOG.info("Read model " + envInPath + " in " + modelReadTime + " milliseconds");
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new HyracksDataException("Exception while reading model", e);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        throw new HyracksDataException("Exception while deserializing model", e);
                    }
                } else {
                    LOG.info("Used shared environment");
                    model = (Model) context.model;
                }
            }

            // Load the examples.
            MapTaskState state = (MapTaskState) IterationUtils.getIterationState(ctx, partition);
            if (state == null) {
                throw new IllegalStateException("Input data was not cached");
            } else {
                // Use the same state in the future iterations
                IterationUtils.removeIterationState(ctx, partition);
                IterationUtils.setIterationState(ctx, partition, state);
            }

            // Compute the aggregates
            // To improve the filesystem cache hit rate under a LRU replacement
            // policy, alternate the read direction on each round.
            boolean readInReverse = roundNum % 2 != 0;
            LOG.info("Can't read in reverse direction");
            readInReverse = false;
            LOG.info("Reading cached input data in " + (readInReverse ? "forwards" : "reverse") + " direction");
            RunFileWriter runFileWriter = state.getRunFileWriter();

            Log.info("Cached example file size is " + runFileWriter.getFileSize() + " bytes");
            final RunFileReader reader = new RunFileReader(runFileWriter.getFileReference(), ctx.getIOManager(),
                    runFileWriter.getFileSize());
            //readInReverse
            reader.open();
            final ByteBuffer inputFrame = fileCtx.allocateFrame();
            ChunkFrameHelper chunkFrameHelper = new ChunkFrameHelper(ctx);
            IMapFunctionFactory<Model> factory = imruSpec.getMapFunctionFactory();
            IMRUContext imruContext = new IMRUContext(chunkFrameHelper.getContext(), name);
            if (factory.useAPI2()) {
                Iterator<ByteBuffer> input = new Iterator<ByteBuffer>() {
                    boolean read = false;
                    boolean hasData;

                    @Override
                    public void remove() {
                    }

                    @Override
                    public ByteBuffer next() {
                        if (!hasNext())
                            return null;
                        read = false;
                        return inputFrame;
                    }

                    @Override
                    public boolean hasNext() {
                        try {
                            if (!read) {
                                hasData = reader.nextFrame(inputFrame);
                                read = true;
                            }
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                        }
                        return hasData;
                    }
                };
                writer = chunkFrameHelper.wrapWriter(writer, partition);
                IMapFunction2 mapFunction = factory.createMapFunction2(imruContext, imruSpec.getCachedDataFrameSize(),
                        model);
                mapFunction.map(input, writer);
            } else {
                IMapFunction mapFunction = factory.createMapFunction(imruContext, imruSpec.getCachedDataFrameSize(),
                        model);
                writer = chunkFrameHelper.wrapWriter(writer, partition);
                mapFunction.open();
                mapFunction.setFrameWriter(writer);
                while (reader.nextFrame(inputFrame)) {
                    mapFunction.map(inputFrame);
                }
                mapFunction.close();
            }
            writer.close();
        }

    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new MapOperatorNodePushable<Model>(ctx, imruSpec, envInPath, confFactory, partition, roundNum, name
                + " " + partition + "/" + nPartitions);
    }

}
