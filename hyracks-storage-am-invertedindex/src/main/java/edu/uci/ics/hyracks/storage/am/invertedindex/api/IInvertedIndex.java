package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;

public interface IInvertedIndex extends IIndex {
    public IInvertedListCursor createInvertedListCursor();

    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference tupleReference)
            throws HyracksDataException, IndexException;

    public IBinaryComparatorFactory[] getInvListElementCmpFactories();

    public ITypeTraits[] getTypeTraits();
}
