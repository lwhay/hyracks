package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;

public class LSMInvertedIndexRangeSearchCursor implements IIndexCursor {

    private LSMHarness harness;
    private boolean includeMemComponent;
    private AtomicInteger searcherRefCount;
    private List<IIndexAccessor> indexAccessors;
    private List<IIndexCursor> indexCursors;
    private ICursorInitialState initialState;
    private boolean flagEOF = false;
    private boolean flagFirstNextCall = true;
    
    private PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    private MultiComparator cmp;
    private PriorityQueueComparator pqCmp;
    private PriorityQueueElement outputElement;
    
    public LSMInvertedIndexRangeSearchCursor() {
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
		for (IIndexCursor c : indexCursors) {
			if (c.hasNext()) {
				return  true;
			}
		}
		
		flagEOF = true;
		return false;
	}

	@Override
	public void next() throws HyracksDataException {
		
		if (flagFirstNextCall){
			flagFirstNextCall = false;
			
			//initialize PriorityQueue
	
			
			
			IIndexCursor cursor;
			
			//read the first tuple from each cursor.
			for (int i = 0; i < indexCursors.size(); i++) {
				cursor = indexCursors.get(i);
				if (cursor.hasNext()) {
					cursor.next();
					cursor.getTuple();
				} else {
					//remove from the list
					cursor.close();
					indexCursors.remove(i);
					i--;
				}
			}
		}

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
	
    public class PriorityQueueComparator implements Comparator<PriorityQueueElement> {

        private final MultiComparator cmp;

        public PriorityQueueComparator(MultiComparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result = cmp.compare(elementA.getTuple(), elementB.getTuple());
            if (result != 0) {
                return result;
            }
            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }

        public MultiComparator getMultiComparator() {
            return cmp;
        }
    }
    
    public class PriorityQueueElement {
        private ITupleReference tuple;
        private int cursorIndex;
        
        public PriorityQueueElement(ITupleReference tuple, int cursorIndex) {
            reset(tuple, cursorIndex);
        }

        public ITupleReference getTuple() {
            return tuple;
        }

        public int getCursorIndex() {
            return cursorIndex;
        }
        
        public void reset(ITupleReference tuple, int cursorIndex) {
            this.tuple = tuple;
            this.cursorIndex = cursorIndex;
        }
    }

}
