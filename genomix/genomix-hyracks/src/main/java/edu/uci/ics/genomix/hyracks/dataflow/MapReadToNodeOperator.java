package edu.uci.ics.genomix.hyracks.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.genomix.hyracks.data.primitive.NodeReference;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class MapReadToNodeOperator extends AbstractSingleActivityOperatorDescriptor {

    public MapReadToNodeOperator(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc, int kmerSize) {
        super(spec, 1, 1);
        recordDescriptors[0] = outRecDesc;
        this.kmerSize = kmerSize;
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final int kmerSize;

    public static final int InputReadIDField = 0;
    public static final int InputInfoFieldStart = 1;

    public static final int OutputNodeIDField = 0;
    public static final int OutputCountOfKmerField = 1;
    public static final int OutputForwardForwardField = 2;
    public static final int OutputForwardReverseField = 3;
    public static final int OutputReverseForwardField = 4;
    public static final int OutputReverseReverseField = 5;
    public static final int OutputKmerBytesField = 6;

    public static final RecordDescriptor nodeOutputRec = new RecordDescriptor(new ISerializerDeserializer[7]);

    /**
     * (ReadID, Storage[posInRead]={len, PositionList, len, Kmer})
     * to (Position, LengthCount, InComingPosList, OutgoingPosList, Kmer)
     */
    public class MapReadToNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        public static final int INT_LENGTH = 4;
        private final IHyracksTaskContext ctx;
        private final RecordDescriptor inputRecDesc;
        private final RecordDescriptor outputRecDesc;

        private final int LAST_POSITION_ID;

        private FrameTupleAccessor accessor;
        private ByteBuffer writeBuffer;
        private ArrayTupleBuilder builder;
        private FrameTupleAppender appender;

        private NodeReference curNodeEntry;
        private NodeReference nextNodeEntry;
        private NodeReference nextNextNodeEntry;

        private PositionListWritable cachePositionList;

        public MapReadToNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
                RecordDescriptor outputRecDesc) {
            this.ctx = ctx;
            this.inputRecDesc = inputRecDesc;
            this.outputRecDesc = outputRecDesc;
            curNodeEntry = new NodeReference(kmerSize);
            nextNodeEntry = new NodeReference(kmerSize);
            nextNextNodeEntry = new NodeReference(0);
            cachePositionList = new PositionListWritable();
            LAST_POSITION_ID = inputRecDesc.getFieldCount() - InputInfoFieldStart;
        }

        @Override
        public void open() throws HyracksDataException {
            accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
            writeBuffer = ctx.allocateFrame();
            builder = new ArrayTupleBuilder(outputRecDesc.getFieldCount());
            appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(writeBuffer, true);
            writer.open();
            curNodeEntry.reset(kmerSize);
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            accessor.reset(buffer);
            int tupleCount = accessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                generateNodeFromRead(i);
            }
        }

        private void generateNodeFromRead(int tIndex) throws HyracksDataException {
            int offsetPoslist = accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength();
            int readID = accessor.getBuffer().getInt(
                    offsetPoslist + accessor.getFieldStartOffset(tIndex, InputReadIDField));
            if ((accessor.getFieldCount() - InputInfoFieldStart) % 2 != 0) {
                throw new IllegalArgumentException("field length is odd");
            }

            resetNode(curNodeEntry, readID, (byte) (1));
            setForwardIncomingList(curNodeEntry,
                    offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart));
            setKmer(curNodeEntry.getKmer(), offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart));
            if (curNodeEntry.getNodeID().getPosInRead() == LAST_POSITION_ID) {
                setReverseIncomingList(curNodeEntry,
                        offsetPoslist + accessor.getFieldStartOffset(tIndex, InputInfoFieldStart + 1));
            }
            for (int i = InputInfoFieldStart + 2; i < accessor.getFieldCount(); i += 2) {
                // next Node
                if (i + 2 < accessor.getFieldCount()) {
                    setForwardOutgoingList(curNodeEntry, offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 2));
                    resetNode(nextNodeEntry, readID, (byte) (1 + (i - InputInfoFieldStart) / 2));
                    setKmer(nextNodeEntry.getKmer(), offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 2));
                    setReverseOutgoingList(nextNodeEntry, offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 1));
                    if (nextNodeEntry.getNodeID().getPosInRead() == LAST_POSITION_ID) {
                        setReverseIncomingList(nextNodeEntry,
                                offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 3));
                    }
                } else {
                    resetNode(nextNodeEntry, readID, (byte) 0);
                }
                // nextNext node
                if (i + 4 < accessor.getFieldCount()) {
                    setForwardOutgoingList(nextNodeEntry, offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 4));
                    resetNode(nextNextNodeEntry, readID, (byte) (2 + (i - InputInfoFieldStart) / 2));
                    setReverseOutgoingList(nextNextNodeEntry,
                            offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 3));
                    if (nextNextNodeEntry.getNodeID().getPosInRead() == LAST_POSITION_ID) {
                        setReverseIncomingList(nextNextNodeEntry,
                                offsetPoslist + accessor.getFieldStartOffset(tIndex, i + 5));
                    }
                } else {
                    resetNode(nextNextNodeEntry, readID, (byte) 0);
                }

                if (curNodeEntry.inDegree() > 1 || curNodeEntry.outDegree() > 1 || nextNodeEntry.inDegree() > 1
                        || nextNodeEntry.outDegree() > 1 || nextNextNodeEntry.inDegree() > 1
                        || nextNextNodeEntry.outDegree() > 1) {
                    connect(curNodeEntry, nextNodeEntry);
                    outputNode(curNodeEntry);
                    curNodeEntry.set(nextNodeEntry);
                    continue;
                }
                curNodeEntry.mergeForwadNext(nextNodeEntry, kmerSize);
            }
            outputNode(curNodeEntry);
        }

        private void setKmer(KmerBytesWritable kmer, int offset) {
            ByteBuffer buffer = accessor.getBuffer();
            int length = buffer.getInt(offset);
            offset += INT_LENGTH + length;
            length = buffer.getInt(offset);
            if (kmer.getLength() != length) {
                throw new IllegalArgumentException("kmer size is invalid");
            }
            offset += INT_LENGTH;
            kmer.set(buffer.array(), offset);
        }

        private void connect(NodeReference curNode, NodeReference nextNode) {
            curNode.getFFList().append(nextNode.getNodeID());
            nextNode.getRRList().append(curNode.getNodeID());
        }

        private void setCachList(int offset) {
            ByteBuffer buffer = accessor.getBuffer();
            int count = PositionListWritable.getCountByDataLength(buffer.getInt(offset));
            cachePositionList.set(count, buffer.array(), offset + INT_LENGTH);
        }

        private void resetNode(NodeReference node, int readID, byte posInRead) {
            node.reset(kmerSize);
            node.setNodeID(readID, posInRead);
        }

        private void setReverseOutgoingList(NodeReference node, int offset) {
            setCachList(offset);
            for (PositionWritable pos : cachePositionList) {
                if (pos.getPosInRead() > 0) {
                    node.getRFList().append(pos);
                } else {
                    node.getRRList().append(pos.getReadID(), (byte) -pos.getPosInRead());
                }
            }
        }

        private void setReverseIncomingList(NodeReference node, int offset) {
            setCachList(offset);
            for (PositionWritable pos : cachePositionList) {
                if (pos.getPosInRead() > 0) {
                    if (pos.getPosInRead() > 1) {
                        node.getFRList().append(pos.getReadID(), (byte) (pos.getPosInRead() - 1));
                    } else {
                        throw new IllegalArgumentException("Invalid position");
                    }
                } else {
                    if (pos.getPosInRead() > -LAST_POSITION_ID) {
                        node.getFFList().append(pos.getReadID(), (byte) -(pos.getPosInRead() - 1));
                    }
                }
            }
        }

        private void setForwardOutgoingList(NodeReference node, int offset) {
            setCachList(offset);
            for (PositionWritable pos : cachePositionList) {
                if (pos.getPosInRead() > 0) {
                    node.getFFList().append(pos);
                } else {
                    node.getFRList().append(pos.getReadID(), (byte) -pos.getPosInRead());
                }
            }
        }

        private void setForwardIncomingList(NodeReference node, int offset) {
            setCachList(offset);
            for (PositionWritable pos : cachePositionList) {
                if (pos.getPosInRead() > 0) {
                    if (pos.getPosInRead() > 1) {
                        node.getRRList().append(pos.getReadID(), (byte) (pos.getPosInRead() - 1));
                    } else {
                        throw new IllegalArgumentException("position id is invalid");
                    }
                } else {
                    if (pos.getPosInRead() > -LAST_POSITION_ID) {
                        node.getRFList().append(pos.getReadID(), (byte) -(pos.getPosInRead() - 1));
                    }
                }
            }
        }

        private void outputNode(NodeReference node) throws HyracksDataException {
            if (node.getNodeID().getPosInRead() == 0) {
                return;
            }
            try {
                builder.addField(node.getNodeID().getByteArray(), node.getNodeID().getStartOffset(), node.getNodeID()
                        .getLength());
                builder.getDataOutput().writeInt(node.getCount());
                builder.addFieldEndOffset();
                builder.addField(node.getFFList().getByteArray(), node.getFFList().getStartOffset(), node.getFFList()
                        .getLength());
                builder.addField(node.getFRList().getByteArray(), node.getFRList().getStartOffset(), node.getFRList()
                        .getLength());
                builder.addField(node.getRFList().getByteArray(), node.getRFList().getStartOffset(), node.getRFList()
                        .getLength());
                builder.addField(node.getRRList().getByteArray(), node.getRRList().getStartOffset(), node.getRRList()
                        .getLength());
                builder.addField(node.getKmer().getBytes(), node.getKmer().getOffset(), node.getKmer().getLength());

                if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                    FrameUtils.flushFrame(writeBuffer, writer);
                    appender.reset(writeBuffer, true);
                    if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                        throw new IllegalStateException("Failed to append tuplebuilder to frame");
                    }
                }
                builder.reset();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to Add a field to the tupleBuilder.");
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeBuffer, writer);
            }
            writer.close();
        }

    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new MapReadToNodePushable(ctx, recordDescProvider.getInputRecordDescriptor(getActivityId(), 0),
                recordDescriptors[0]);
    }

}