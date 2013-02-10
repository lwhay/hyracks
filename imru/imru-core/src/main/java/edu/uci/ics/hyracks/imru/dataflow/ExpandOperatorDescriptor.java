package edu.uci.ics.hyracks.imru.dataflow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api2.IIMRUJobSpecificationImpl;
import edu.uci.ics.hyracks.imru.base.IConfigurationFactory;
import edu.uci.ics.hyracks.imru.data.ChunkFrameHelper;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ExpandOperatorDescriptor extends IMRUOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final RecordDescriptor dummyRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[1]);

    private final IIMRUJobSpecification<?> imruSpec;
    public int level = 0;
    private final String envInPath;
    private final String envOutPath;

    public ExpandOperatorDescriptor(JobSpecification spec, IIMRUJobSpecification<?> imruSpec, String modelInPath,
            IConfigurationFactory confFactory, String envOutPath, String name, boolean lastLevel) {
        super(spec, 1, lastLevel ? 0 : 1, name, imruSpec, confFactory);
        this.imruSpec = imruSpec;
        this.envInPath = modelInPath;
        this.envOutPath = envOutPath;
        if (!lastLevel)
            recordDescriptors[0] = dummyRecordDescriptor;
    }

    private static class ExpandOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private final IIMRUJobSpecification<?> imruSpec;
        private final ChunkFrameHelper chunkFrameHelper;
        private final List<List<ByteBuffer>> bufferedChunks;
        private final int partition;
        //        private IReduceFunction reduceFunction;
        public String name;
        IMRUContext imruContext;
        public int level = 0;
        private final IConfigurationFactory confFactory;
        private Configuration conf;
        private final String modelPath;
        private final String outPath;

        public ExpandOperatorNodePushable(IHyracksTaskContext ctx, IIMRUJobSpecification<?> imruSpec, int partition,
                String modelPath, IConfigurationFactory confFactory, String outPath, String name, int level) {
            this.imruSpec = imruSpec;
            this.name = name;
            this.modelPath = modelPath;
            this.outPath = outPath;
            this.confFactory = confFactory;
            this.chunkFrameHelper = new ChunkFrameHelper(ctx);
            this.bufferedChunks = new ArrayList<List<ByteBuffer>>();
            this.partition = partition;
            this.level = level;
        }

        @Override
        public void open() throws HyracksDataException {
            writer.open();
            imruContext = new IMRUContext(chunkFrameHelper.getContext(), name);
            Rt.p("expand " + imruContext.getNodeId());
            //            reduceFunction = imruSpec.getReduceFunctionFactory().createReduceFunction(imruContext);
            writer = chunkFrameHelper.wrapWriter(writer, partition);
            //            reduceFunction.setFrameWriter(writer);
            //            reduceFunction.open();
        }

        @Override
        public void nextFrame(ByteBuffer encapsulatedChunk) throws HyracksDataException {
            ByteBuffer chunk = chunkFrameHelper.extractChunk(encapsulatedChunk);
            int senderPartition = chunkFrameHelper.getPartition(encapsulatedChunk);
            boolean isLastChunk = chunkFrameHelper.isLastChunk(encapsulatedChunk);
            enqueueChunk(chunk, senderPartition);
            if (isLastChunk) {
                byte[] objectData = IIMRUJobSpecificationImpl.deserializeFromChunks(imruContext,
                        bufferedChunks.remove(senderPartition));
                NCApplicationContext appContext = (NCApplicationContext) imruContext.getJobletContext()
                        .getApplicationContext();
                try {
                    Serializable model = (Serializable) appContext.deserialize(objectData);
                    OutputStream fileOutput;
                    conf = confFactory == null ? null : confFactory.createConfiguration();
                    String path2 = outPath.replaceAll(Pattern.quote("${NODE_ID}"), imruContext.getNodeId());
                    if (conf == null) {
                        File file = new File(path2);
                        fileOutput = new FileOutputStream(file);
                    } else {
                        FileSystem dfs = FileSystem.get(conf);
                        Path path = new Path(path2);
                        fileOutput = dfs.create(path, true);
                    }

                    ObjectOutputStream output = new ObjectOutputStream(fileOutput);
                    output.writeObject(model);
                    output.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            //            reduceFunction.close();
            writer.close();
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
        return new ExpandOperatorNodePushable(ctx, imruSpec, partition, envInPath, confFactory, envOutPath,
                this.getDisplayName() + partition, level);
    }
}
