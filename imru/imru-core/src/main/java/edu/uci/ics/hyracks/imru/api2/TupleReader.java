package edu.uci.ics.hyracks.imru.api2;

import java.io.DataInputStream;

import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.imru.example.utils.R;

public class TupleReader extends DataInputStream {
    FrameTupleAccessor accessor;
    ByteBufferInputStream in;
    int tupleId;

    public TupleReader(FrameTupleAccessor accessor, ByteBufferInputStream in) {
        super(in);
        this.accessor = accessor;
        this.in = in;
    }

    public void seekToField(int fieldId) {
        int startOffset = accessor.getFieldSlotsLength()
                + accessor.getTupleStartOffset(tupleId)
                + accessor.getFieldStartOffset(tupleId, fieldId);
//        R.printHex(0, accessor.getBuffer().array(), startOffset, 256);
//        R.p("offset " + startOffset);
        in.setByteBuffer(accessor.getBuffer(), startOffset);
    }
}
