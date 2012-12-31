package edu.uci.ics.hyracks.imru.api2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMRUJob<Model, IntermediateResult> extends Serializable {
    /**
     * Return initial model
     */
    public Model initModel();

    /**
     * Frame size must be large enough to store at least one tuple
     */
    public int getCachedDataFrameSize();

    /**
     * Number of fields for each tuple
     */
    public int getFieldCount();

    /**
     * Parse input data and output tuples
     */
    public void parse(IHyracksTaskContext ctx, InputStream input,
            TupleWriter output) throws IOException;

    /**
     * For each tuple, return one result.
     * Or by using nextTuple(), return one result
     * after processing multiple tuples.
     */
    public IntermediateResult map(TupleReader input, Model model,
            int cachedDataFrameSize) throws IOException;

    /**
     * Combine multiple results to one result
     */
    public IntermediateResult reduce(Iterator<IntermediateResult> input)
            throws HyracksDataException;

    /**
     * update the model using combined result
     */
    public void update(Iterator<IntermediateResult> input, Model model)
            throws HyracksDataException;

    /**
     * Return true to exit loop
     */
    public boolean shouldTerminate(Model model);
}
