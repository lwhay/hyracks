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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.api.IOneByOneUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IReassemblingUpdateFunction;
import edu.uci.ics.hyracks.imru.api.IUpdateFunction;
import edu.uci.ics.hyracks.imru.base.IConfigurationFactory;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.util.MemoryStatsLogger;

/**
 * Evaluates the update function in an iterative map reduce update
 * job.
 * <p>
 * The updated model is serialized to a file in HDFS, where it is read by the driver and mappers.
 * 
 * @param <Model>
 *            Josh Rosen
 */
public class UpdateOperatorDescriptor<Model extends IModel> extends IMRUOperatorDescriptor<Model> {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(UpdateOperatorDescriptor.class.getName());

    private final String envInPath;
    private final String envOutPath;

    /**
     * Create a new UpdateOperatorDescriptor.
     * 
     * @param spec
     *            The job specification
     * @param imruSpec
     *            The IMRU job specification
     * @param modelInPath
     *            The HDFS path to read the current model from
     * @param confFactory
     *            A Hadoop configuration, used for HDFS.
     * @param envOutPath
     *            The HDFS path to serialize the updated environment
     *            to.
     */
    public UpdateOperatorDescriptor(JobSpecification spec, IIMRUJobSpecification<Model> imruSpec, String modelInPath,
            IConfigurationFactory confFactory, String envOutPath) {
        super(spec, 1, 0, "update", imruSpec, confFactory);
        this.envInPath = modelInPath;
        this.envOutPath = envOutPath;
    }

    private static class UpdateOperatorNodePushable<Model extends IModel> extends
            AbstractUnaryInputSinkOperatorNodePushable {

        private final IIMRUJobSpecification<Model> imruSpec;
        private final String modelPath;
        private final String outPath;
        private final IConfigurationFactory confFactory;
        private final ChunkFrameHelper chunkFrameHelper;
        private final List<List<ByteBuffer>> bufferedChunks;

        private IUpdateFunction updateFunction;
        private Configuration conf;
        private Model model;
        private final String name;

        public UpdateOperatorNodePushable(IHyracksTaskContext ctx, IIMRUJobSpecification<Model> imruSpec,
                String modelPath, IConfigurationFactory confFactory, String outPath, String name) {
            this.imruSpec = imruSpec;
            this.modelPath = modelPath;
            this.outPath = outPath;
            this.confFactory = confFactory;
            this.name = name;
            this.chunkFrameHelper = new ChunkFrameHelper(ctx);
            this.bufferedChunks = new ArrayList<List<ByteBuffer>>();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void open() throws HyracksDataException {
            MemoryStatsLogger.logHeapStats(LOG, "Update: Initializing Update");
            conf = confFactory == null ? null : confFactory.createConfiguration();
            // Load the model
            try {
                InputStream fileInput;
                if (conf == null) {
                    fileInput = new FileInputStream(modelPath);
                } else {
                    FileSystem dfs = FileSystem.get(conf);
                    fileInput = dfs.open(new Path(modelPath));
                }

                ObjectInputStream input = new ObjectInputStream(fileInput);
                model = (Model) input.readObject();
                input.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            } catch (ClassNotFoundException e) {
                throw new HyracksDataException(e);
            }
            IMRUContext imruContext = new IMRUContext(chunkFrameHelper.getContext(), name);
            updateFunction = imruSpec.getUpdateFunctionFactory().createUpdateFunction(imruContext, model);
            updateFunction.open();
        }

        @Override
        public void nextFrame(ByteBuffer encapsulatedChunk) throws HyracksDataException {
            ByteBuffer chunk = chunkFrameHelper.extractChunk(encapsulatedChunk);
            if (updateFunction instanceof IOneByOneUpdateFunction) {
                ((IOneByOneUpdateFunction) updateFunction).update(chunk);
            } else if (updateFunction instanceof IReassemblingUpdateFunction) {
                int senderPartition = chunkFrameHelper.getPartition(encapsulatedChunk);
                boolean isLastChunk = chunkFrameHelper.isLastChunk(encapsulatedChunk);
                enqueueChunk(chunk, senderPartition);
                if (isLastChunk) {
                    ((IReassemblingUpdateFunction) updateFunction).update(bufferedChunks.remove(senderPartition));
                }
            } else {
                throw new HyracksDataException("Unknown IUpdateFunction interface");
            }
        }

        @Override
        public void fail() throws HyracksDataException {
        }

        @Override
        public void close() throws HyracksDataException {
            updateFunction.close();
            // Serialize the model so it can be sent back to the
            // driver program.
            long start = System.currentTimeMillis();
            try {
                OutputStream fileOutput;
                if (conf == null) {
                    File file = new File(outPath);
                    fileOutput = new FileOutputStream(file);
                } else {
                    FileSystem dfs = FileSystem.get(conf);
                    Path path = new Path(outPath);
                    fileOutput = dfs.create(path, true);
                }

                ObjectOutputStream output = new ObjectOutputStream(fileOutput);
                output.writeObject(model);
                output.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            long end = System.currentTimeMillis();
            LOG.info("Wrote updated model to HDFS " + (end - start) + " milliseconds");
            MemoryStatsLogger.logHeapStats(LOG, "Update: Deinitializing Update");
        }

        private void enqueueChunk(ByteBuffer chunk, int senderPartition) {
            if (bufferedChunks.size() <= senderPartition) {
                for (int i = bufferedChunks.size(); i <= senderPartition; i++) {
                    bufferedChunks.add(new LinkedList<ByteBuffer>());
                }
            }
            bufferedChunks.get(senderPartition).add(chunk);
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new UpdateOperatorNodePushable<Model>(ctx, imruSpec, envInPath, confFactory, envOutPath, this
                .getDisplayName()
                + partition);
    }

}
