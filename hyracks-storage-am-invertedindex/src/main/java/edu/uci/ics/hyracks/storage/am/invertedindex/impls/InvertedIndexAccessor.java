package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IIOManager;
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

    public InvertedIndexAccessor(IInvertedIndex index, IBinaryTokenizer tokenizer) {
        this.searcher = new TOccurrenceSearcher(ctx, index, tokenizer);
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

    public IInvertedIndexSearcher getSearcher() {
        return searcher;
    }

    // This is just a dummy hyracks context for allocating frames for temporary
    // results during inverted index searches.
    // TODO: In the future we should use the real HyracksTaskContext to track
    // frame usage.
    private class DefaultHyracksCommonContext implements IHyracksCommonContext {
        private final int FRAME_SIZE = 32768;

        @Override
        public ByteBuffer allocateFrame() {
            return ByteBuffer.allocate(FRAME_SIZE);
        }

        @Override
        public int getFrameSize() {
            return FRAME_SIZE;
        }

        @Override
        public IIOManager getIOManager() {
            return null;
        }
    }
}
