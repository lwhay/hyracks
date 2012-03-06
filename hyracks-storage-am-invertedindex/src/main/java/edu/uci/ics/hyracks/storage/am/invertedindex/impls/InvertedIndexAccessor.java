package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;

public class InvertedIndexAccessor implements IIndexAccessor {
    private final IHyracksCommonContext ctx = new DefaultHyracksCommonContext();
    private final IInvertedIndexSearcher searcher;
    private final IInvertedIndex invIndex;

    public InvertedIndexAccessor(IInvertedIndex index, IBinaryTokenizer tokenizer) {
        this.searcher = new TOccurrenceSearcher(ctx, index, tokenizer);
        this.invIndex = index;
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub
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
        return new InvertedIndexSearchCursor(searcher);
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        searcher.search((InvertedIndexSearchCursor) cursor, (InvertedIndexSearchPredicate) searchPred);
    }
    
    public IIndexCursor createRangeSearchCursor() {
        return new InvertedIndexRangeSearchCursor(invIndex);
    }

    public void rangeSearch(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        cursor.open(null, searchPred);
    }

    public IInvertedIndexSearcher getSearcher() {
        return searcher;
    }
}
