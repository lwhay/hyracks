package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper;

public class HashGrouper implements IPushBasedGrouper {

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#init()
     */
    @Override
    public void init() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#getDataDistHistogram()
     */
    @Override
    public int[] getDataDistHistogram() throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#flush(edu.uci.ics.hyracks.api.comm.IFrameWriter)
     */
    @Override
    public void flush(IFrameWriter writer) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#reset()
     */
    @Override
    public void reset() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#close()
     */
    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

}
