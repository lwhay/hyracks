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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.ASyncIO;
import edu.uci.ics.hyracks.imru.api.ASyncInputStream;
import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.FrameWriter;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.TupleReader;
import edu.uci.ics.hyracks.imru.data.RunFileContext;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.MapTaskState;
import edu.uci.ics.hyracks.imru.util.IterationUtils;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Parses input data from files in HDFS and caches it on the local
 * file system. During IMRU iterations, these cached examples are
 * processed by the Map operator.
 * 
 * @author Josh Rosen
 */
public class DataLoadOperatorDescriptor extends
        IMRUOperatorDescriptor<Serializable, Serializable> {
    private static final Logger LOG = Logger
            .getLogger(MapOperatorDescriptor.class.getName());

    private static final long serialVersionUID = 1L;

    protected final ConfigurationFactory confFactory;
    protected final IMRUFileSplit[] inputSplits;
    private boolean hdfsLoad = false;
    private boolean memCache;

    /**
     * Create a new DataLoadOperatorDescriptor.
     * 
     * @param spec
     *            The Hyracks job specification for the dataflow
     * @param imruSpec
     *            The IMRU job specification
     * @param inputSplits
     *            The files to read the input records from
     * @param confFactory
     *            A Hadoop configuration, used for HDFS.
     */
    public DataLoadOperatorDescriptor(JobSpecification spec,
            IIMRUJob2<Serializable, Serializable> imruSpec,
            IMRUFileSplit[] inputSplits, ConfigurationFactory confFactory,
            boolean hdfsLoad, boolean memCache) {
        super(spec, hdfsLoad ? 1 : 0, 0, "parse", imruSpec);
        this.inputSplits = inputSplits;
        this.confFactory = confFactory;
        this.hdfsLoad = hdfsLoad;
        this.memCache = memCache;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            int nPartitions) throws HyracksDataException {
        return new AbstractOperatorNodePushable() {
            private final IHyracksTaskContext fileCtx;
            private final String name;
            long startTime;
            MapTaskState state;
            RunFileWriter runFileWriter;
            DataWriter dataWriter;
            IMRUContext imruContext;
            boolean initialized = false;

            {
                fileCtx = new RunFileContext(ctx, imruSpec
                        .getCachedDataFrameSize());
                name = DataLoadOperatorDescriptor.this.getDisplayName()
                        + partition;
            }

            @Override
            public void initialize() throws HyracksDataException {
                //                Rt.p("initialize");
                if (initialized)
                    return;
                initialized = true;
                // Load the examples.
                state = (MapTaskState) IterationUtils.getIterationState(ctx,
                        partition);
                if (state != null) {
                    LOG.severe("Duplicate loading of input data.");
                    INCApplicationContext appContext = ctx.getJobletContext()
                            .getApplicationContext();
                    IMRURuntimeContext context = (IMRURuntimeContext) appContext
                            .getApplicationObject();
                    context.modelAge = 0;
                    //                throw new IllegalStateException("Duplicate loading of input data.");
                }
                startTime = System.currentTimeMillis();
                if (state == null)
                    state = new MapTaskState(ctx.getJobletContext().getJobId(),
                            ctx.getTaskAttemptId().getTaskId());
                if (!memCache) {
                    FileReference file = ctx
                            .createUnmanagedWorkspaceFile("IMRUInput");
                    runFileWriter = new RunFileWriter(file, ctx.getIOManager());
                    state.setRunFileWriter(runFileWriter);
                    runFileWriter.open();
                } else {
                    Vector vector = new Vector();
                    state.setMemCache(vector);
                    dataWriter = new DataWriter<Serializable>(vector);
                }

                imruContext = new IMRUContext(fileCtx, name);
                if (!hdfsLoad) {
                    final IMRUFileSplit split = inputSplits[partition];
                    try {
                        InputStream in = split.getInputStream();
                        if (runFileWriter != null) {
                            imruSpec.parse(imruContext,
                                    new BufferedInputStream(in, 1024 * 1024),
                                    new FrameWriter(runFileWriter));
                        } else {
                            imruSpec.parse(imruContext,
                                    new BufferedInputStream(in, 1024 * 1024),
                                    dataWriter);
                        }
                        in.close();
                    } catch (IOException e) {
                        fail();
                        Rt.p(imruContext.getNodeId() + " " + split);
                        throw new HyracksDataException(e);
                    }
                    finishDataCache();
                }
            }

            void finishDataCache() throws HyracksDataException {
                if (runFileWriter != null) {
                    runFileWriter.close();
                    LOG.info("Cached input data file "
                            + runFileWriter.getFileReference().getFile()
                                    .getAbsolutePath() + " is "
                            + runFileWriter.getFileSize() + " bytes");
                }
                long end = System.currentTimeMillis();
                LOG.info("Parsed input data in " + (end - startTime)
                        + " milliseconds");
                IterationUtils.setIterationState(ctx, partition, state);
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer,
                    RecordDescriptor recordDesc) {
                throw new IllegalArgumentException();
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }

            @Override
            public int getInputArity() {
                return hdfsLoad ? 1 : 0;
            }

            @Override
            public final IFrameWriter getInputFrameWriter(int index) {
                //                Rt.p("getInputFrameWriter");
                try {
                    initialize();
                } catch (HyracksDataException e1) {
                    e1.printStackTrace();
                }
                return new IFrameWriter() {
                    private ASyncIO<byte[]> io;
                    private ASyncInputStream stream;
                    Future future;
                    byte[] ln = "\n".getBytes();

                    @Override
                    public void open() throws HyracksDataException {
                        io = new ASyncIO<byte[]>();
                        stream = new ASyncInputStream(io);
                        future = IMRUSerialize.threadPool
                                .submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            if (runFileWriter != null) {
                                                imruSpec.parse(imruContext,
                                                        stream,
                                                        new FrameWriter(
                                                                runFileWriter));
                                            } else {
                                                imruSpec.parse(imruContext,
                                                        stream, dataWriter);
                                            }

                                            stream.close();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                });
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer)
                            throws HyracksDataException {
                        try {
                            TupleReader reader = new TupleReader(buffer, ctx
                                    .getFrameSize(), 1);
                            while (reader.nextTuple()) {
                                //                                reader.dump();
                                int len = reader.getFieldLength(0);
                                reader.seekToField(0);
                                byte[] bs = new byte[len];
                                reader.readFully(bs);
                                io.add(bs);
                                //                                io.add(ln);
                                //                                String word = new String(bs);
                                //                                Rt.p(word);
                            }
                            reader.close();
                        } catch (IOException ex) {
                            throw new HyracksDataException(ex);
                        }
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        io.close();
                        try {
                            future.get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        finishDataCache();
                    }
                };
            }

            private void fail() throws HyracksDataException {
            }
        };
    }
}