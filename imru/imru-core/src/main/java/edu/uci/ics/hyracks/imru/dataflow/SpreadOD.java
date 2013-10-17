package edu.uci.ics.hyracks.imru.dataflow;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializer;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.jobgen.SpreadGraph;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRURuntimeContext;
import edu.uci.ics.hyracks.imru.util.Rt;

public class SpreadOD extends AbstractSingleActivityOperatorDescriptor {
    private static Logger LOG = Logger.getLogger(SpreadOD.class.getName());
    SpreadGraph.Level level;
    boolean first;
    boolean last;
    int roundNum;
    String modelName;
    IMRUConnection imruConnection;
    String dataFilePath;
    DeploymentId deploymentId;

    public SpreadOD(DeploymentId deploymentId, JobSpecification spec,
            SpreadGraph.Level[] levels, int level, String modelName,
            IMRUConnection imruConnection, int roundNum, String dataFilePath) {
        super(spec, level > 0 ? 1 : 0, level < levels.length - 1 ? 1 : 0);
        this.level = levels[level];
        this.deploymentId = deploymentId;
        this.modelName = modelName;
        this.imruConnection = imruConnection;
        this.roundNum = roundNum;
        this.dataFilePath = dataFilePath;
        first = level == 0;
        last = level == levels.length - 1;
        if (!last)
            recordDescriptors[0] = new RecordDescriptor(
                    new ISerializerDeserializer[1]);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            int nPartitions) throws HyracksDataException {
        if (first) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    SpreadOD.this.nextFrame(ctx, writer, partition, null, null);
                }
            };
        } else if (last) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                Hashtable<Integer, LinkedList<ByteBuffer>> queue = new Hashtable<Integer, LinkedList<ByteBuffer>>();

                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void nextFrame(ByteBuffer buffer)
                        throws HyracksDataException {
                    SpreadOD.this.nextFrame(ctx, writer, partition, buffer,
                            queue);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                }
            };
        } else {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                Hashtable<Integer, LinkedList<ByteBuffer>> queue = new Hashtable<Integer, LinkedList<ByteBuffer>>();

                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void nextFrame(ByteBuffer buffer)
                        throws HyracksDataException {
                    SpreadOD.this.nextFrame(ctx, writer, partition, buffer,
                            queue);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                }
            };
        }
    }

    public void nextFrame(IHyracksTaskContext ctx, IFrameWriter writer,
            int partition, ByteBuffer buffer,
            Hashtable<Integer, LinkedList<ByteBuffer>> hash)
            throws HyracksDataException {
        int frameSize = ctx.getFrameSize();
        MergedFrames frames = MergedFrames.nextFrame(ctx, buffer, hash);
        if (!first && frames == null)
            return;
        if (!last)
            writer.open();
        try {
            IMRUContext imruContext = new IMRUContext(ctx);
            String nodeId = imruContext.getNodeId();
            byte[] bs;
            if (first) {
                bs = imruConnection.downloadData(modelName);
                LOG.info("download model at " + nodeId + " round " + roundNum);
            } else {
                bs = frames.data;
            }
            if (dataFilePath == null || dataFilePath.length() == 0) {
                IJobSerializerDeserializer jobSerDe = ctx.getJobletContext()
                        .getApplicationContext()
                        .getJobSerializerDeserializerContainer()
                        .getJobSerializerDeerializer(deploymentId);
                Serializable model = (Serializable) jobSerDe.deserialize(bs);
                imruContext.setModel(model, roundNum);
            } else {
                File file = new File(dataFilePath);
                if (!file.getParentFile().exists())
                    file.getParentFile().mkdirs();
                Rt.write(file, bs);
            }
            SpreadGraph.Node node = level.nodes.get(partition);

            ByteBuffer frame = ctx.allocateFrame();
            for (SpreadGraph.Node n : node.subNodes) {
                //                        node.print(0);
                //                        Rt.p(to.nodes.get(partition).name + " " + new IMRUContext(ctx).getNodeId() + " to " + node.name);
                //                buffer.putInt(0, n.partitionInThisLevel);
                MergedFrames
                        .serializeToFrames(imruContext, frame, frameSize,
                                writer, bs, node.partitionInThisLevel,
                                n.partitionInThisLevel,
                                node.partitionInThisLevel, null);
                if (last)
                    throw new Error();
                //                writer.nextFrame(buffer);
            }
        } catch (Exception e) {
            if (!last)
                writer.fail();
            throw new HyracksDataException(e);
        } finally {
            if (!last)
                writer.close();
        }
    }
}
