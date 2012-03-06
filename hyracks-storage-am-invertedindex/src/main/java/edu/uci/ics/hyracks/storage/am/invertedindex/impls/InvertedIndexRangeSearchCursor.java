package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class InvertedIndexRangeSearchCursor implements IIndexCursor {

	private final BTree btree;
	private final ITreeIndexAccessor btreeAccessor;
	private final IInvertedIndex invIndex;
	private IInvertedListCursor invListCursor;
	
	public InvertedIndexRangeSearchCursor (IInvertedIndex invIndex) {
		this.btree = ((InvertedIndex) invIndex).getBTree();
		this.btreeAccessor = btree.createAccessor();
		this.invIndex = invIndex;		
	}
	
	@Override
	public void open(ICursorInitialState initialState,
			ISearchPredicate searchPred) throws HyracksDataException {
		this.invListCursor = invIndex.createInvertedListCursor();
		
		//get token from btree
		
		//openInvertedListCursor
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
