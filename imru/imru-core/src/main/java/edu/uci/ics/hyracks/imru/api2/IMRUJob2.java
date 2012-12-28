package edu.uci.ics.hyracks.imru.api2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMRUJob2<Model> extends Serializable {
    public Model initModel();

    public int getCachedDataFrameSize();

    public void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws IOException;

    public void map(Iterator<ByteBuffer> input, Model model,
            OutputStream output, int cachedDataFrameSize)
            throws HyracksDataException;

    public void reduce(IHyracksTaskContext ctx, Iterator<byte[]> input,
            OutputStream output) throws HyracksDataException;

    public void update(IHyracksTaskContext ctx, Iterator<byte[]> input,
            Model model) throws HyracksDataException;

    public boolean shouldTerminate(Model model);
}
