package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.GroupJoinHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class InMemoryHashGroupJoin {
    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    private final FrameTupleAppender appender;
    private final IBinaryComparatorFactory[] comparator;
    private final ByteBuffer outBuffer;
	private final GroupJoinHashTable gByTable;

    public InMemoryHashGroupJoin(IHyracksTaskContext ctx, int tableSize, FrameTupleAccessor accessor0,
            FrameTupleAccessor accessor1, IBinaryComparatorFactory[] groupComparator, ITuplePartitionComputerFactory gByTpc0,
            ITuplePartitionComputerFactory gByTpc1, RecordDescriptor gByInRecordDescriptor, RecordDescriptor gByOutRecordDescriptor,
            IAggregatorDescriptorFactory aggregatorFactory, int[] joinAttributes, int[] groupAttributes, 
            INullWriter[] nullWriters1) throws HyracksDataException {
        buffers = new ArrayList<ByteBuffer>();
        this.accessorBuild = accessor0;
        this.accessorProbe = accessor1;
        appender = new FrameTupleAppender(ctx.getFrameSize());
        this.comparator = groupComparator;
        
        outBuffer = ctx.allocateFrame();
        appender.reset(outBuffer, true);

        gByTable = new GroupJoinHashTable(ctx, groupAttributes, joinAttributes, comparator, gByTpc0, gByTpc1, 
        		aggregatorFactory, gByInRecordDescriptor, gByOutRecordDescriptor, nullWriters1, tableSize);
    }

    public void build(ByteBuffer buffer) throws HyracksDataException, IOException {
        buffers.add(buffer);
        gByTable.build(accessorBuild, buffer);
    }

    public void join(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        gByTable.insert(accessorProbe, buffer);
    }

    public void write(IFrameWriter writer) throws HyracksDataException {
    	gByTable.write(writer);
    }
    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            flushFrame(outBuffer, writer);
        }
    }

    private void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        writer.nextFrame(buffer);
        buffer.position(0);
        buffer.limit(buffer.capacity());
    }
}