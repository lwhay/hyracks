/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class InMemoryBtreeInvertedIndex implements IInvertedIndex {

    private BTree btree;
    private IBufferCache bufferCache;
    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final IInvertedListBuilder invListBuilder;
    private final IBinaryTokenizer tokenizer;
    private final int numTokenFields;
    private final int numInvListKeys;

    private final RangePredicate btreePred;
    private final IBTreeLeafFrame leafFrame;
    private final ITreeIndexCursor btreeCursor;
    private final MultiComparator searchCmp;

    public InMemoryBtreeInvertedIndex(IBufferCache bufferCache, BTree btree, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, IInvertedListBuilder invListBuilder,
            IBinaryTokenizer tokenizer) {
        this.bufferCache = bufferCache;
        this.btree = btree;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.invListBuilder = invListBuilder;
        this.tokenizer = tokenizer;
        this.numTokenFields = btree.getComparatorFactories().length;
        this.numInvListKeys = invListCmpFactories.length;

        // setup for cursor creation
        btreePred = new RangePredicate(null, null, true, true, null, null);
        leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        btreeCursor = new BTreeRangeSearchCursor(leafFrame, false);
        searchCmp = MultiComparator.create(btree.getComparatorFactories());
        btreePred.setLowKeyComparator(searchCmp);
        btreePred.setHighKeyComparator(searchCmp);
    }

    @Override
    public void open(int fileId) {
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
    }

    @Override
    public void close() {
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        return new InMemoryBtreeInvertedListCursor(btreeCursor);
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, IFrameTupleReference tupleReference)
            throws HyracksDataException, IndexException {
        btreePred.setLowKey(tupleReference, true);
        btreePred.setHighKey(tupleReference, true);

        ITreeIndexAccessor btreeAccessor = btree.createAccessor();

        btreeAccessor.search(btreeCursor, btreePred);
    }

    @Override
    public IIndexAccessor createAccessor() {
        return new InvertedIndexAccessor(this, tokenizer);
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws IndexException, HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.INVERTED;
    }

    @Override
    public IBinaryComparatorFactory[] getInvListElementCmpFactories() {
        return invListCmpFactories;
    }

    @Override
    public ITypeTraits[] getTypeTraits() {
        return invListTypeTraits;
    }

}
