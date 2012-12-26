package edu.uci.ics.hyracks.imru.api2;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

abstract public class IMRUJob<Model, IntermediateResult> implements
        Serializable {
    abstract public IntermediateResult map(Iterator<ByteBuffer> input,
            Model model, int cachedDataFrameSize) throws HyracksDataException;

    abstract public IntermediateResult reduce(Iterator<IntermediateResult> input)
            throws HyracksDataException;

    abstract public void update(Iterator<IntermediateResult> input, Model model)
            throws HyracksDataException;

    abstract public Model initModel();

    public abstract int getCachedDataFrameSize();

    public abstract void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException;

    public abstract boolean shouldTerminate(Model model);

}
