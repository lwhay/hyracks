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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexComponentFinalizer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndex implements ILSMIndex {

    private final LSMHarness lsmHarness;
    private final IInvertedIndex memoryInvertedIndex;
    private final BTreeFactory diskBTreeFactory;
    private final InvertedIndexFactory diskInvertedIndexFactory;
    private final ILSMFileManager fileManager;
    private final IFileMapProvider diskFileMapProvider;
    private final ILSMComponentFinalizer componentFinalizer;
    private LinkedList<Object> diskInvertedIndexList = new LinkedList<Object>();
    private final IBufferCache diskBufferCache;

    public LSMInvertedIndex(IInvertedIndex memoryBTreeInvertedIndex, BTreeFactory diskBTreeFactory,
            InvertedIndexFactory diskInvertedIndexFactory, ILSMFileManager fileManager,
            IFileMapProvider diskFileMapProvider) {
        this.memoryInvertedIndex = memoryBTreeInvertedIndex;
        this.diskBTreeFactory = diskBTreeFactory;
        this.diskInvertedIndexFactory = diskInvertedIndexFactory;
        this.fileManager = fileManager;
        this.diskFileMapProvider = diskFileMapProvider;
        this.lsmHarness = new LSMHarness(this);
        this.componentFinalizer = new TreeIndexComponentFinalizer(diskFileMapProvider);
        this.diskBufferCache = diskBTreeFactory.getBufferCache();
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void open(int indexFileId) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    public IIndexAccessor createAccessor() {
        return new LSMInvertedIndexAccessor(lsmHarness, createOpContext());
    }

    public LSMInvertedIndexOpContext createOpContext() {
        return new LSMInvertedIndexOpContext(memoryInvertedIndex);
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws IndexException, HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public IBufferCache getBufferCache() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IndexType getIndexType() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            IndexException {

        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;

        try {
            ctx.getAccessor().insert(tuple);
        } catch (BTreeDuplicateKeyException e) {
            // This case should never happen in InMemoryBTreeInvertedIndex.
        }

        return true;
    }

    @Override
    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, IndexException {
        IIndexAccessor componentAccessor;

        // Over-provision by 1 if includeMemComponent == false, but that's okay!
        ArrayList<IIndexAccessor> indexAccessors = new ArrayList<IIndexAccessor>(diskComponents.size() + 1);

        if (includeMemComponent) {
            componentAccessor = memoryInvertedIndex.createAccessor();
            indexAccessors.add(componentAccessor);
        }

        for (int i = 0; i < diskComponents.size(); i++) {
            componentAccessor = ((IInvertedIndex) diskComponents.get(i)).createAccessor();
            indexAccessors.add(componentAccessor);
        }

        LSMInvertedIndexCursorInitialState initState = new LSMInvertedIndexCursorInitialState(indexAccessors,
                includeMemComponent, searcherRefCount, lsmHarness);
        LSMInvertedIndexSearchCursor lsmCursor = (LSMInvertedIndexSearchCursor) cursor;
        lsmCursor.open(initState, pred);
    }

    @Override
    public Object merge(List<Object> mergedComponents) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addMergedComponent(Object newComponent, List<Object> mergedComponents) {
        // TODO Auto-generated method stub

    }

    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public Object flush() throws HyracksDataException, IndexException {

        // ---------------------------------------------------
        // [Flow]
        // #. Create a scanCursor of memoryInvertedIndex to iterate all keys in it.
        // #. Create an diskInvertedIndex where all keys of memoryInvertedIndex will be bulkloaded.
        //    - Create a BTree instance for diskBTree 
        //    - Create an InvertedIndex instance
        // #. Begin the bulkload of the diskInvertedIndex.
        // #. While iterating the scanCursor, add each key into the diskInvertedIndex in the bulkload mode.
        // #. End the bulkload.
        // #. Return the newly created diskInvertedIndex.
        // ---------------------------------------------------

        // #. Create a scanCursor of memoryInvertedIndex to iterate all keys in it.
        ILSMIndexAccessor memoryInvertedIndexAccessor = (ILSMIndexAccessor) memoryInvertedIndex.createAccessor();
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        IIndexCursor scanCursor = memoryInvertedIndexAccessor.createSearchCursor();
        memoryInvertedIndexAccessor.search(scanCursor, nullPred);

        // #. Create a diskInvertedIndex where all keys of memoryInvertedIndex will be bulkloaded.
        //    - Create a BTree instance for diskBTree
        BTree diskBTree = createBTreeFlushTarget();
        //    - Create an InvertedIndex instance
        InvertedIndex diskInvertedIndex = createInvertedIndexFlushTarget(diskBTree);
        
        // #. Begin the bulkload of the diskInvertedIndex.
        IIndexBulkLoadContext bulkLoadCtx = diskInvertedIndex.beginBulkLoad(1.0f);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                diskInvertedIndex.bulkLoadAddTuple(scanCursor.getTuple(), bulkLoadCtx);
            }
        } finally {
            scanCursor.close();
        }
        diskInvertedIndex.endBulkLoad(bulkLoadCtx);        

        return diskInvertedIndex;
    }

    private BTree createBTreeFlushTarget() throws HyracksDataException {
        FileReference fileRef = fileManager.createFlushFile((String) fileManager.getRelFlushFileName());
        return createDiskBTree(fileRef, true);
    }

    private BTree createDiskBTree(FileReference fileRef, boolean createBTree) throws HyracksDataException {
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(fileRef);
        int diskBTreeFileId = diskFileMapProvider.lookupFileId(fileRef);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskBTreeFileId);
        // Create new BTree instance.
        BTree diskBTree = diskBTreeFactory.createIndexInstance();
        if (createBTree) {
            diskBTree.create(diskBTreeFileId);
        }
        // BTree will be closed during cleanup of merge().
        diskBTree.open(diskBTreeFileId);
        return diskBTree;
    }

    private InvertedIndex createInvertedIndexFlushTarget(BTree diskBTree) throws HyracksDataException {
        FileReference fileRef = fileManager.createFlushFile((String) fileManager.getRelFlushFileName());
        return createDiskInvertedIndex(fileRef, true, diskBTree);
    }

    private InvertedIndex createDiskInvertedIndex(FileReference fileRef, boolean createInvertedIndex, BTree diskBTree)
            throws HyracksDataException {
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(fileRef);
        int diskInvertedIndexFileId = diskFileMapProvider.lookupFileId(fileRef);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskInvertedIndexFileId);
        // Create new InvertedIndex instance.
        InvertedIndex diskInvertedIndex = (InvertedIndex) diskInvertedIndexFactory.createIndexInstance(diskBTree);
        if (createInvertedIndex) {
            diskInvertedIndex.create(diskInvertedIndexFileId);
        }
        // InvertedIndex will be closed during cleanup of merge().
        diskInvertedIndex.open(diskInvertedIndexFileId);
        return diskInvertedIndex;
    }

    public void addFlushedComponent(Object index) {
        diskInvertedIndexList.addFirst(index);
    }

    @Override
    public InMemoryFreePageManager getInMemoryFreePageManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public List<Object> getDiskComponents() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ILSMComponentFinalizer getComponentFinalizer() {
        // TODO Auto-generated method stub
        return null;
    }

}
