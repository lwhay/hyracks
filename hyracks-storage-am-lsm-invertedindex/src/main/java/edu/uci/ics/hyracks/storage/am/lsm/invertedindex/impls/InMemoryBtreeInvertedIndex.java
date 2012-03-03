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
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;
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
    private final ArrayTupleBuilder btreeTupleBuilder;
    private final ArrayTupleReference btreeTupleReference;

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
        this.btreePred = new RangePredicate(null, null, true, true, null, null);
        this.leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        this.btreeCursor = new BTreeRangeSearchCursor(leafFrame, false);
        this.searchCmp = MultiComparator.create(btree.getComparatorFactories());
        this.btreePred.setLowKeyComparator(searchCmp);
        this.btreePred.setHighKeyComparator(searchCmp);
        
        // To generate in-memory BTree tuples 
        this.btreeTupleBuilder = new ArrayTupleBuilder(btree.getFieldCount());
        this.btreeTupleReference = new ArrayTupleReference();
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
    
    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException, TreeIndexException {        
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;
        // TODO: This will become much simpler once the BTree supports a true upsert operation.

        //Tuple --> |Field1|Field2| ... |FieldN|doc-id|
        //Each field represents a document and doc-id always comes at the last field.
        //parse document
        //create a list of (term,doc-id)
        //sort the list in the order of term
        //insert a pair of (term, doc-id) into in-memory BTree until to the end of the list.
        
        byte[] docID = tuple.getFieldData(numTokenFields-1);
        int docIDLength = tuple.getFieldLength(numTokenFields-1);
        
        for (int i=0; i < numTokenFields; i++)
        {
            tokenizer.reset(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            while (tokenizer.hasNext()) {
                tokenizer.next();
                IToken token = tokenizer.getToken();
                btreeTupleBuilder.reset();
                btreeTupleBuilder.addField(token.getData(), token.getStart(), token.getTokenLength());
                btreeTupleBuilder.addField(docID, 0, docIDLength);
                btreeTupleReference.reset(btreeTupleBuilder.getFieldEndOffsets(), btreeTupleBuilder.getByteArray());

                try {
                    btree.createAccessor().insert(btreeTupleReference);
                } catch (IndexException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return true;
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
