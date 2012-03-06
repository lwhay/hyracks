package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;

public class LSMInvertedIndexRangeSearchCursor implements IIndexCursor {

    private int cursorIndex;
    private LSMHarness harness;
    private boolean includeMemComponent;
    private AtomicInteger searcherRefCount;
    private List<IIndexAccessor> indexAccessors;
    private List<IIndexCursor> indexCursors;
    private ICursorInitialState initialState;
    
    public LSMInvertedIndexRangeSearchCursor() {
        cursorIndex = -1;

    }
    
	@Override
	public void open(ICursorInitialState initialState,
			ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexCursorInitialState lsmInitialState = (LSMInvertedIndexCursorInitialState) initialState;
        harness = lsmInitialState.getLSMHarness();
        includeMemComponent = lsmInitialState.getIncludeMemComponent();
        searcherRefCount = lsmInitialState.getSearcherRefCount();
        indexAccessors = lsmInitialState.getIndexAccessors();
        indexCursors = new ArrayList<IIndexCursor>(indexAccessors.size());
        this.initialState = initialState;

        //create all cursors
        IIndexCursor cursor;
        for (IIndexAccessor a : indexAccessors) {
        	InvertedIndexAccessor invIndexAccessor = (InvertedIndexAccessor) a;
            cursor = invIndexAccessor.createRangeSearchCursor();
            try {
            	invIndexAccessor.rangeSearch(cursor, searchPred);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            indexCursors.add(cursor);
        }
	}

	@Override
	public boolean hasNext() throws HyracksDataException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void next() throws HyracksDataException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws HyracksDataException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public ITupleReference getTuple() {
		// TODO Auto-generated method stub
		return null;
	}

}
