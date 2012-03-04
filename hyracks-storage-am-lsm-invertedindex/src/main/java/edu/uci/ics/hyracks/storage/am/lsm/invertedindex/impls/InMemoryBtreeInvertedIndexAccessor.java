package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class InMemoryBtreeInvertedIndexAccessor implements ILSMIndexAccessor {

    protected IIndexOpContext ctx;
    protected InMemoryBtreeInvertedIndex memoryBtreeInvertedIndex;
    
    public InMemoryBtreeInvertedIndexAccessor(InMemoryBtreeInvertedIndex memoryBtreeInvertedIndex, IIndexOpContext ctx) {
        this.ctx = ctx;
        this.memoryBtreeInvertedIndex = memoryBtreeInvertedIndex;
    }
    
    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        memoryBtreeInvertedIndex.insertUpdateOrDelete(tuple, ctx);
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub

    }

    @Override
    public IIndexCursor createSearchCursor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub

    }

    @Override
    public void flush() throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub

    }

    @Override
    public void merge() throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub

    }

}
