package edu.uci.ics.hyracks.imru.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;
import edu.uci.ics.hyracks.dataflow.std.connectors.PartitionDataWriter;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.jobgen.SpreadGraph;
import edu.uci.ics.hyracks.imru.util.Rt;

public class SpreadConnectorDescriptor extends AbstractMToNConnectorDescriptor {
    private static final long serialVersionUID = 1L;
    SpreadGraph.Level from;
    SpreadGraph.Level to;

    public SpreadConnectorDescriptor(IConnectorDescriptorRegistry spec,
            SpreadGraph.Level from, SpreadGraph.Level to) {
        super(spec);
        this.from = from;
        this.to = to;
    }

    @Override
    public IFrameWriter createPartitioner(final IHyracksTaskContext ctx,
            RecordDescriptor recordDesc,
            final IPartitionWriterFactory edwFactory,
            final int senderPartition, int nProducerPartitions,
            final int consumerPartitionCount) throws HyracksDataException {
        return new IFrameWriter() {
            private final IFrameWriter[] pWriters;

            {
                pWriters = new IFrameWriter[consumerPartitionCount];
                for (int i = 0; i < consumerPartitionCount; ++i) {
                    try {
                        pWriters[i] = edwFactory.createFrameWriter(i);
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void close() throws HyracksDataException {
                for (int i = 0; i < pWriters.length; ++i) {
                    pWriters[i].close();
                }
            }

            private void flushFrame(ByteBuffer buffer, IFrameWriter frameWriter)
                    throws HyracksDataException {
                buffer.position(0);
                buffer.limit(buffer.capacity());
                frameWriter.nextFrame(buffer);
            }

            @Override
            public void open() throws HyracksDataException {
                for (int i = 0; i < pWriters.length; ++i) {
                    pWriters[i].open();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                int targetPartition = buffer.getInt(MergedFrames.TARGET_OFFSET);
                flushFrame(buffer, pWriters[targetPartition]);
                //                if (from != null)
                //                    Rt.p("Level " + from.level + "->" + to.level + ": " + senderPartition + " "
                //                            + from.nodes.get(senderPartition) + "->" + targetPartition + " "
                //                            + to.nodes.get(targetPartition).name);
                //                    Rt.p(from.nodes.get(senderPartition) + "->" + to.nodes.get(targetPartition).name);
            }

            @Override
            public void fail() throws HyracksDataException {
                for (int i = 0; i < pWriters.length; ++i) {
                    pWriters[i].fail();
                }
            }
        };
    }

    @Override
    public IPartitionCollector createPartitionCollector(
            IHyracksTaskContext ctx, RecordDescriptor recordDesc, int index,
            int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(0, nProducerPartitions);
        NonDeterministicChannelReader channelReader = new NonDeterministicChannelReader(
                nProducerPartitions, expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(
                channelReader);
        return new PartitionCollector(ctx, getConnectorId(), index,
                expectedPartitions, frameReader, channelReader);
    }
}