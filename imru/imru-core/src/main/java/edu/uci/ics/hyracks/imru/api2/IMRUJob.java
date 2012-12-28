package edu.uci.ics.hyracks.imru.api2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMRUJob<Model, IntermediateResult> extends Serializable {
    public Model initModel();

    public int getCachedDataFrameSize();

    public int getFieldCount();

    public void parse(IHyracksTaskContext ctx, InputStream input,
            TupleWriter output) throws IOException;

    public IntermediateResult map(TupleReader input, Model model,
            int cachedDataFrameSize) throws IOException;

    public IntermediateResult reduce(Iterator<IntermediateResult> input)
            throws HyracksDataException;

    public void update(Iterator<IntermediateResult> input, Model model)
            throws HyracksDataException;

    public boolean shouldTerminate(Model model);

}
