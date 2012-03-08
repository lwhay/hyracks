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

package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

/**
 * An inverted index consists of two files: 1. a file storing (paginated)
 * inverted lists 2. a BTree-file mapping from tokens to inverted lists.
 * Implemented features: bulk loading and searching (based on T-Occurrence) Not
 * implemented features: updates (insert/update/delete) Limitations: a query
 * cannot exceed the size of a Hyracks frame.
 */
public class InvertedIndex implements IInvertedIndex {
    private BTree btree;
    private int rootPageId = 0;
    private IBufferCache bufferCache;
    private int fileId;
    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final IInvertedListBuilder invListBuilder;
    private final IBinaryTokenizer tokenizer;
    private final int numTokenFields;
    private final int numInvListKeys;

    private final RangePredicate btreePred;
    private final ITreeIndexFrame leafFrame;
    private final ITreeIndexCursor btreeCursor;
    private final MultiComparator searchCmp;

    public InvertedIndex(IBufferCache bufferCache, BTree btree, ITypeTraits[] invListTypeTraits,
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
        leafFrame = btree.getLeafFrameFactory().createFrame();
        btreeCursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) leafFrame, false);
        searchCmp = MultiComparator.create(btree.getComparatorFactories());
        btreePred.setLowKeyComparator(searchCmp);
        btreePred.setHighKeyComparator(searchCmp);
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
    }

    @Override
    public void close() {
        this.fileId = -1;
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        return new FixedSizeElementInvertedListCursor(bufferCache, fileId, invListTypeTraits);
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference tupleReference)
            throws HyracksDataException, IndexException {
        btreePred.setLowKey(tupleReference, true);
        btreePred.setHighKey(tupleReference, true);

        ITreeIndexAccessor btreeAccessor = btree.createAccessor();

        btreeAccessor.search(btreeCursor, btreePred);

        try {
            if (btreeCursor.hasNext()) {
                btreeCursor.next();
                ITupleReference frameTuple = btreeCursor.getTuple();
                // Hardcoded mapping of btree fields
                int startPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(1),
                        frameTuple.getFieldStart(1));
                int endPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(2),
                        frameTuple.getFieldStart(2));
                int startOff = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(3),
                        frameTuple.getFieldStart(3));
                int numElements = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(4),
                        frameTuple.getFieldStart(4));
                listCursor.reset(startPageId, endPageId, startOff, numElements);
            } else {
                listCursor.reset(0, 0, 0, 0);
            }
        } finally {
            btreeCursor.close();
            btreeCursor.reset();
        }
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
        InvertedIndexBulkLoadContext ctx = new InvertedIndexBulkLoadContext(fillFactor);
        ctx.init(rootPageId, fileId);
        return ctx;
    }

    /**
     * Assumptions:
     * The first btree.getMultiComparator().getKeyFieldCount() fields in tuple
     * are btree keys (e.g., a string token).
     * The next invListCmp.getKeyFieldCount() fields in tuple are keys of the
     * inverted list (e.g., primary key).
     * Key fields of inverted list are fixed size.
     */
    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        InvertedIndexBulkLoadContext ctx = (InvertedIndexBulkLoadContext) ictx;

        // First inverted list, copy token to baaos and start new list.
        if (ctx.currentInvListTokenBaaos.size() == 0) {
            ctx.currentInvListStartPageId = ctx.currentPageId;
            ctx.currentInvListStartOffset = invListBuilder.getPos();

            // Remember current token.
            ctx.currentInvListTokenBaaos.reset();
            for (int i = 0; i < numTokenFields; i++) {
                ctx.currentInvListTokenBaaos.write(tuple.getFieldData(i), tuple.getFieldStart(i),
                        tuple.getFieldLength(i));
            }

            if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                ctx.pinNextPage();
                invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
                if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                    throw new IllegalStateException("Failed to create first inverted list.");
                }
            }
        }

        // Create new inverted list?
        ctx.currentInvListToken.reset(ctx.currentInvListTokenBaaos.getByteArray(), 0);
        if (ctx.tokenCmp.compare(tuple, ctx.currentInvListToken) != 0) {

            // Create entry in btree for last inverted list.
            createAndInsertBTreeTuple(ctx);

            // Remember new token.
            ctx.currentInvListTokenBaaos.reset();
            for (int i = 0; i < numTokenFields; i++) {
                ctx.currentInvListTokenBaaos.write(tuple.getFieldData(i), tuple.getFieldStart(i),
                        tuple.getFieldLength(i));
            }

            // Start new list.
            if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                ctx.pinNextPage();
                invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
                if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                    throw new IllegalStateException("Failed to start new inverted list after switching to a new page.");
                }
            }

            ctx.currentInvListStartPageId = ctx.currentPageId;
            ctx.currentInvListStartOffset = invListBuilder.getPos();
        }

        // Append to current inverted list.
        if (!invListBuilder.appendElement(tuple, numTokenFields, numInvListKeys)) {
            ctx.pinNextPage();
            invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
            if (!invListBuilder.appendElement(tuple, numTokenFields, numInvListKeys)) {
                throw new IllegalStateException(
                        "Failed to append element to inverted list after switching to a new page.");
            }
        }
    }

    private void createAndInsertBTreeTuple(InvertedIndexBulkLoadContext ctx) throws HyracksDataException {
        // Build tuple.        
        ctx.btreeTupleBuilder.reset();
        ctx.btreeTupleBuilder.addField(ctx.currentInvListTokenBaaos.getByteArray(), 0,
                ctx.currentInvListTokenBaaos.size());
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, ctx.currentInvListStartPageId);
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, ctx.currentPageId);
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, ctx.currentInvListStartOffset);
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, invListBuilder.getListSize());
        // Reset tuple reference and add it.
        ctx.btreeTupleReference.reset(ctx.btreeTupleBuilder.getFieldEndOffsets(), ctx.btreeTupleBuilder.getByteArray());
        btree.bulkLoadAddTuple(ctx.btreeTupleReference, ctx.btreeBulkLoadCtx);
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        // Create entry in btree for last inverted list.
        InvertedIndexBulkLoadContext ctx = (InvertedIndexBulkLoadContext) ictx;
        createAndInsertBTreeTuple(ctx);
        btree.endBulkLoad(ctx.btreeBulkLoadCtx);
        ctx.deinit();
    }

    public final class InvertedIndexBulkLoadContext implements IIndexBulkLoadContext {
        private final ArrayTupleBuilder btreeTupleBuilder;
        private final ArrayTupleReference btreeTupleReference;
        private final float btreeFillFactor;
        private IIndexBulkLoadContext btreeBulkLoadCtx;

        private int currentInvListStartPageId;
        private int currentInvListStartOffset;
        private final ByteArrayAccessibleOutputStream currentInvListTokenBaaos = new ByteArrayAccessibleOutputStream();
        private final FixedSizeTupleReference currentInvListToken = new FixedSizeTupleReference(invListTypeTraits);

        private int currentPageId;
        private ICachedPage currentPage;
        private final MultiComparator tokenCmp;

        public InvertedIndexBulkLoadContext(float btreeFillFactor) {
            this.tokenCmp = MultiComparator.create(btree.getComparatorFactories());
            this.btreeTupleBuilder = new ArrayTupleBuilder(btree.getFieldCount());
            this.btreeTupleReference = new ArrayTupleReference();
            this.btreeFillFactor = btreeFillFactor;
        }

        public void init(int startPageId, int fileId) throws HyracksDataException, TreeIndexException {
            btreeBulkLoadCtx = btree.beginBulkLoad(btreeFillFactor);
            currentPageId = startPageId;
            currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
            currentPage.acquireWriteLatch();
            invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
        }

        public void deinit() throws HyracksDataException {
            if (currentPage != null) {
                currentPage.releaseWriteLatch();
                bufferCache.unpin(currentPage);
            }
        }

        public void pinNextPage() throws HyracksDataException {
            currentPage.releaseWriteLatch();
            bufferCache.unpin(currentPage);
            currentPageId++;
            currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
            currentPage.acquireWriteLatch();
        }
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public int getInvListsFileId() {
        return fileId;
    }

    public IBinaryComparatorFactory[] getInvListElementCmpFactories() {
        return invListCmpFactories;
    }

    public ITypeTraits[] getTypeTraits() {
        return invListTypeTraits;
    }

    public BTree getBTree() {
        return btree;
    }

    @Override
    public IIndexAccessor createAccessor() {
        return new InvertedIndexAccessor(this, tokenizer);
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.INVERTED;
    }
    
    public int getFileId() {
        return fileId;
    }

}
