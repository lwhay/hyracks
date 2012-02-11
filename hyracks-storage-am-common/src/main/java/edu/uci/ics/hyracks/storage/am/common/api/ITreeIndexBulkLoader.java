package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITreeIndexBulkLoader {
	// FIXME Doku

    /**
     * Append a tuple to the index in the context of a bulk load.
     * 
     * @param tuple
     *            Tuple to be inserted.
     * @param ictx
     *            Existing bulk load context.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws PageAllocationException
     */
    public void add(ITupleReference tuple)
            throws HyracksDataException, PageAllocationException;

    /**
     * Finalize the bulk loading operation in the given context.
     * 
     * @param ictx
     *            Existing bulk load context to be finalized.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws PageAllocationException
     */
    public void end()
            throws HyracksDataException, PageAllocationException;

}
