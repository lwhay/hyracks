package edu.uci.ics.hyracks.imru.api2;

import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMRUJob<Model, IntermediateResult> extends Serializable {
    public Model initModel();

    public void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException;

    public IntermediateResult map(Iterator<ByteBuffer> input, Model model,
            int cachedDataFrameSize) throws HyracksDataException;

    public IntermediateResult reduce(Iterator<IntermediateResult> input)
            throws HyracksDataException;

    public void update(Iterator<IntermediateResult> input, Model model)
            throws HyracksDataException;

    public int getCachedDataFrameSize();

    public boolean shouldTerminate(Model model);

}
