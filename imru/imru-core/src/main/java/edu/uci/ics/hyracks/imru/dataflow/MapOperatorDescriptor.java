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

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.logging.Logger;

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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.data.RunFileContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;
import edu.uci.ics.hyracks.imru.util.MemoryStatsLogger;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Evaluates the map function in an iterative map reduce update job.
 * 
 * @param <Model>
 *            The class used to represent the global model that is
 *            persisted between iterations.
 * @author Josh Rosen
 */
public class MapOperatorDescriptor<Model extends Serializable> extends
        IMRUOperatorDescriptor<Model> {

    private static Logger LOG = Logger.getLogger(MapOperatorDescriptor.class
            .getName());

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(
            new ISerializerDeserializer[1]);

    //    private final String envInPath;
    private final int roundNum;

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
    public MapOperatorDescriptor(JobSpecification spec,
            IIMRUJob2<Model> imruSpec, int roundNum, String name) {
        super(spec, 0, 1, name, imruSpec);
        recordDescriptors[0] = dummyRecordDescriptor;
        this.roundNum = roundNum;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            int nPartitions) throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private final IHyracksTaskContext fileCtx;
            private final String name;

            {
                this.name = MapOperatorDescriptor.this.getDisplayName()
                        + partition;
                fileCtx = new RunFileContext(ctx,
                        imruSpec.getCachedDataFrameSize());
            }

            @SuppressWarnings("unchecked")
            @Override
            public void initialize() throws HyracksDataException {
                MemoryStatsLogger.logHeapStats(LOG,
                        "MapOperator: Before reading examples");
                writer.open();

                // Load the environment and weight vector.
                // For efficiency reasons, the Environment and weight vector are
                // shared across all MapOperator partitions.
                INCApplicationContext appContext = ctx.getJobletContext()
                        .getApplicationContext();
                IMRURuntimeContext context = (IMRURuntimeContext) appContext
                        .getApplicationObject();
                IMRUContext imruContext = new IMRUContext(ctx, name);
                Model model = (Model) context.model;
                synchronized (context.envLock) {
                    if (context.modelAge < roundNum)
                        throw new HyracksDataException(
                                "Model was not spread to "
                                        + imruContext.getNodeId());
                }

                // Load the examples.
                MapTaskState state = (MapTaskState) IterationUtils
                        .getIterationState(ctx, partition);
                if (state == null) {
                    Rt.p("state=null");
                    System.exit(0);
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
                LOG.info("Reading cached input data in "
                        + (readInReverse ? "forwards" : "reverse")
                        + " direction");
                RunFileWriter runFileWriter = state.getRunFileWriter();

                Log.info("Cached example file size is "
                        + runFileWriter.getFileSize() + " bytes");
                final RunFileReader reader = new RunFileReader(
                        runFileWriter.getFileReference(), ctx.getIOManager(),
                        runFileWriter.getFileSize());
                //readInReverse
                reader.open();
                final ByteBuffer inputFrame = fileCtx.allocateFrame();
                ChunkFrameHelper chunkFrameHelper = new ChunkFrameHelper(ctx);
                imruContext = new IMRUContext(chunkFrameHelper.getContext(),
                        name);
                {
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

                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    imruSpec.map(imruContext, input, model, out,
                            imruSpec.getCachedDataFrameSize());
                    byte[] objectData = out.toByteArray();
                    IMRUSerialize.serializeToFrames(imruContext, writer,
                            objectData);
                }
                writer.close();
            }
        };
    }
}
