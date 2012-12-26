package edu.uci.ics.hyracks.imru.api2;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

abstract public class IMRUJob2<Model> implements Serializable {
    abstract public Model initModel();

    public abstract int getCachedDataFrameSize();

    public abstract void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException;

    abstract public void map(Iterator<ByteBuffer> input, Model model,
            OutputStream output, int cachedDataFrameSize)
            throws HyracksDataException;

    abstract public void reduce(IHyracksTaskContext ctx,
            Iterator<byte[]> input, OutputStream output)
            throws HyracksDataException;

    abstract public void update(IHyracksTaskContext ctx,
            Iterator<byte[]> input, Model model) throws HyracksDataException;

    public abstract boolean shouldTerminate(Model model);
}
