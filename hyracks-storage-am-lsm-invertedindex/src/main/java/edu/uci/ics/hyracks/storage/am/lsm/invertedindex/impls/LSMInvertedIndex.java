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

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexFileManager.LSMInvertedIndexFileNameComponent;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndex implements ILSMIndex, IIndex {
	private final Logger LOGGER = Logger.getLogger(LSMInvertedIndex.class.getName());
	
	public class LSMInvertedIndexComponent {
        private final IInvertedIndex invIndex;
        private final BTree deleteKeysBTree;

        LSMInvertedIndexComponent(IInvertedIndex invIndex, BTree deleteKeysBTree) {
            this.invIndex = invIndex;
            this.deleteKeysBTree = deleteKeysBTree;
        }

        public IInvertedIndex getInvIndex() {
            return invIndex;
        }

        public BTree getDeletedKeysBTree() {
            return deleteKeysBTree;
        }
    }
	
    protected final LSMHarness lsmHarness;
    
    // In-memory components.
    protected final LSMInvertedIndexComponent memComponent;
    protected final IBufferCache memBufferCache;
    protected final InMemoryFreePageManager memFreePageManager;
    protected final IBinaryTokenizerFactory tokenizerFactory;
    protected FileReference memDeleteKeysBTreeFile = new FileReference(new File("membtree"));
    
    // On-disk components.
    protected final ILSMIndexFileManager fileManager;
    // For creating inverted indexes in flush and merge.
    protected final OnDiskInvertedIndexFactory diskInvIndexFactory;
    // For creating deleted-keys BTrees in flush and merge.
    protected final BTreeFactory deletedKeysBTreeFactory;
    protected final IBufferCache diskBufferCache;
    protected final IFileMapProvider diskFileMapProvider;
    // List of LSMInvertedIndexComponent instances. Using Object for better sharing via
    // ILSMIndex + LSMHarness.
    protected final LinkedList<Object> diskComponents = new LinkedList<Object>();
    // Helps to guarantees physical consistency of LSM components.
    protected final ILSMComponentFinalizer componentFinalizer;
    
    // Type traits and comparators for tokens and inverted-list elements.
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    
    private boolean isActivated = false;

    public LSMInvertedIndex(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            OnDiskInvertedIndexFactory diskInvIndexFactory, BTreeFactory diskBTreeFactory, ILSMIndexFileManager fileManager,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            ILSMFlushController flushController, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler) throws IndexException {
        InMemoryInvertedIndex memInvIndex = InvertedIndexUtils.createInMemoryBTreeInvertedindex(memBufferCache,
                memFreePageManager, invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories,
                tokenizerFactory);
        BTree deleteKeysBTree = BTreeUtils.createBTree(memBufferCache, diskFileMapProvider, invListTypeTraits,
                invListCmpFactories, BTreeLeafFrameType.REGULAR_NSM, memDeleteKeysBTreeFile);
        memComponent = new LSMInvertedIndexComponent(memInvIndex, deleteKeysBTree);
        this.memBufferCache = memBufferCache;
        this.memFreePageManager = memFreePageManager;
        this.tokenizerFactory = tokenizerFactory;
        this.fileManager = fileManager;
        this.diskInvIndexFactory = diskInvIndexFactory;
        this.deletedKeysBTreeFactory = diskBTreeFactory;
        this.diskBufferCache = diskInvIndexFactory.getBufferCache();
        this.diskFileMapProvider = diskFileMapProvider;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.lsmHarness = new LSMHarness(this, flushController, mergePolicy, opTracker, ioScheduler);
        this.componentFinalizer = new LSMInvertedIndexComponentFinalizer(diskFileMapProvider);
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        fileManager.deleteDirs();
        fileManager.createDirs();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            return;
        }
        try {
            ((InMemoryBufferCache) memComponent.getInvIndex().getBufferCache()).open();
            memComponent.getInvIndex().create();
            memComponent.getDeletedKeysBTree().create();
            List<Object> validFileNames = fileManager.cleanupAndGetValidFiles(componentFinalizer);
            for (Object o : validFileNames) {
                LSMInvertedIndexFileNameComponent component = (LSMInvertedIndexFileNameComponent) o;
                FileReference dictBTreeFile = new FileReference(new File(component.getDictBTreeFileName()));
                FileReference deletedKeysBTreeFile = new FileReference(
                        new File(component.getDeletedKeysBTreeFileName()));
                IInvertedIndex invIndex = createDiskInvIndex(diskInvIndexFactory, dictBTreeFile, false);
                BTree deletedKeysBTree = (BTree) createDiskTree(deletedKeysBTreeFactory, deletedKeysBTreeFile, false);
                LSMInvertedIndexComponent diskComponent = new LSMInvertedIndexComponent(invIndex, deletedKeysBTree);
                diskComponents.add(diskComponent);
            }
            isActivated = true;
            // TODO: Maybe we can make activate throw an index exception?
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }

    protected IInvertedIndex createDiskInvIndex(OnDiskInvertedIndexFactory invIndexFactory, FileReference dictBTreeFileRef, boolean create) throws HyracksDataException, IndexException {
        IInvertedIndex invIndex = invIndexFactory.createIndexInstance(dictBTreeFileRef);
        if (create) {
            invIndex.create();
        }
        // Will be closed during cleanup of merge().
        invIndex.activate();
        return invIndex;
    }
    
    protected ITreeIndex createDiskTree(BTreeFactory btreeFactory, FileReference btreeFileRef, boolean create) throws HyracksDataException, IndexException {
        ITreeIndex btree = btreeFactory.createIndexInstance(btreeFileRef);
        if (create) {
            btree.create();
        }
        // Will be closed during cleanup of merge().
        btree.activate();
        return btree;
    }
    
    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        resetInMemoryComponent();
        for (Object o : diskComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            component.getInvIndex().deactivate();
            component.getDeletedKeysBTree().deactivate();
            component.getInvIndex().destroy();
            component.getDeletedKeysBTree().destroy();
        }
        diskComponents.clear();
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isActivated) {
            return;
        }

        isActivated = false;

        BlockingIOOperationCallback blockingCallBack = new BlockingIOOperationCallback();
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        lsmHarness.getIOScheduler().scheduleOperation(accessor.createFlushOperation(blockingCallBack));
        try {
            blockingCallBack.waitForIO();
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }

        memComponent.getInvIndex().deactivate();
        memComponent.getDeletedKeysBTree().deactivate();
        memComponent.getInvIndex().destroy();
        memComponent.getDeletedKeysBTree().destroy();
        ((InMemoryBufferCache) memComponent.getInvIndex().getBufferCache()).close();
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        memComponent.getInvIndex().destroy();
        memComponent.getDeletedKeysBTree().destroy();        
        for (Object o : diskComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            component.getInvIndex().destroy();
            component.getDeletedKeysBTree().destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public void validate() throws HyracksDataException {
        memComponent.getInvIndex().validate();
        memComponent.getDeletedKeysBTree().validate();
        for (Object o : diskComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            component.getInvIndex().validate();
            component.getDeletedKeysBTree().validate();
        }
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        // TODO: Ignore opcallbacks for now.
        return new LSMInvertedIndexAccessor(lsmHarness, createOpContext());
    }
    
    private LSMInvertedIndexOpContext createOpContext() {
        return new LSMInvertedIndexOpContext(memComponent.getInvIndex(), memComponent.getDeletedKeysBTree());
    }
    
    @Override
    public long getInMemorySize() {
        InMemoryBufferCache memBufferCache = (InMemoryBufferCache) memComponent.getInvIndex().getBufferCache();
        return memBufferCache.getNumPages() * memBufferCache.getPageSize();
    }

    @Override
    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object merge(List<Object> mergedComponents, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput) throws IndexException {
        // TODO Auto-generated method stub
        return null;
    }
    
    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            IndexException {
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;
        ctx.insertAccessor.insert(tuple);
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

        LSMInvertedIndexCursorInitialState initState = new LSMInvertedIndexCursorInitialState(indexAccessors, ictx,
                includeMemComponent, searcherRefCount, lsmHarness);
        LSMInvertedIndexSearchCursor lsmCursor = (LSMInvertedIndexSearchCursor) cursor;
        lsmCursor.open(initState, pred);
    }

    public void mergeSearch(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred,
            IIndexOpContext ictx, boolean includeMemComponent, AtomicInteger searcherRefCount)
            throws HyracksDataException, IndexException {
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

        LSMInvertedIndexCursorInitialState initState = new LSMInvertedIndexCursorInitialState(indexAccessors, ictx,
                includeMemComponent, searcherRefCount, lsmHarness);
        LSMInvertedIndexRangeSearchCursor rangeSearchCursor = (LSMInvertedIndexRangeSearchCursor) cursor;
        rangeSearchCursor.open(initState, pred);
    }

    @Override
    public Object merge(List<Object> mergedComponents) throws HyracksDataException, IndexException {
        LSMInvertedIndexOpContext ctx = createOpContext();

        IIndexCursor cursor = new LSMInvertedIndexRangeSearchCursor();
        RangePredicate mergePred = new RangePredicate(null, null, true, true, null, null);

        //Scan diskInvertedIndexes ignoring the memoryInvertedIndex.
        List<Object> mergingComponents = lsmHarness.mergeSearch(cursor, mergePred, ctx, false);
        mergedComponents.addAll(mergingComponents);

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all diskInvertedIndexes into the new diskInvertedIndex.
        LSMInvertedFileNameComponent fNameComponent = getMergeTargetFileName(mergedComponents);
        BTree diskBTree = createDiskBTree(fileManager.createMergeFile(fNameComponent.getDictBTreeFileName()), true);
        //    - Create an InvertedIndex instance
        OnDiskInvertedIndex mergedDiskInvertedIndex = createDiskInvertedIndex(
                fileManager.createMergeFile(fNameComponent.getInvertedFileName()), true, diskBTree);

        IIndexBulkLoadContext bulkLoadCtx = mergedDiskInvertedIndex.beginBulkLoad(1.0f);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                mergedDiskInvertedIndex.bulkLoadAddTuple(tuple, bulkLoadCtx);
            }
        } finally {
            cursor.close();
        }
        mergedDiskInvertedIndex.endBulkLoad(bulkLoadCtx);

        return mergedDiskInvertedIndex;
    }

    private LSMInvertedFileNameComponent getMergeTargetFileName(List<Object> mergingDiskTrees)
            throws HyracksDataException {
        BTree firstTree = ((OnDiskInvertedIndex) mergingDiskTrees.get(0)).getBTree();
        BTree lastTree = ((OnDiskInvertedIndex) mergingDiskTrees.get(mergingDiskTrees.size() - 1)).getBTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastTree.getFileId());
        LSMInvertedFileNameComponent component = (LSMInvertedFileNameComponent) ((LSMInvertedIndexFileManager) fileManager)
                .getRelMergeFileName(firstFile.getFile().getName(), lastFile.getFile().getName());
        return component;
    }

    @Override
    public void addMergedComponent(Object newComponent, List<Object> mergedComponents) {
        diskComponents.removeAll(mergedComponents);
        diskComponents.addLast(newComponent);
    }

    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        for (Object o : mergedComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            BTree oldDeletedKeysBTree = component.getDeletedKeysBTree();
            oldDeletedKeysBTree.deactivate();
            oldDeletedKeysBTree.destroy();
            IInvertedIndex oldInvIndex = component.getInvIndex();
            oldInvIndex.deactivate();
            oldInvIndex.destroy();
        }
    }

    @Override
    public Object flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMInvertedIndexFlushOperation flushOp = (LSMInvertedIndexFlushOperation) operation;

        // Create an inverted index instance to be bulk loaded.
        IInvertedIndex diskInvertedIndex = createDiskInvIndex(diskInvIndexFactory, flushOp.getDictBTreeFlushTarget(),
                true);

        // Create a scan cursor on the BTree underlying the in-memory inverted index.
        InMemoryInvertedIndexAccessor memInvIndexAccessor = (InMemoryInvertedIndexAccessor) memComponent.getInvIndex()
                .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeAccessor memBTreeAccessor = memInvIndexAccessor.getBTreeAccessor();
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        IIndexCursor scanCursor = memBTreeAccessor.createSearchCursor();
        memBTreeAccessor.search(scanCursor, nullPred);

        // Bulk load the disk inverted index from the in-memory inverted index.
        IIndexBulkLoader invIndexBulkLoader = diskInvertedIndex.createBulkLoader(1.0f, false);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                invIndexBulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
        }
        invIndexBulkLoader.end();

        // Create an BTree instance for the deleted keys.
        BTree diskDeletedKeysBTree = (BTree) createDiskTree(deletedKeysBTreeFactory,
                flushOp.getDeletedKeysBTreeFlushTarget(), true);

        // Create a scan cursor on the deleted keys BTree underlying the in-memory inverted index.
        IIndexAccessor deletedKeysBTreeAccessor = (InMemoryInvertedIndexAccessor) memComponent.getDeletedKeysBTree()
                .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IIndexCursor deletedKeysScanCursor = deletedKeysBTreeAccessor.createSearchCursor();
        deletedKeysBTreeAccessor.search(deletedKeysScanCursor, nullPred);

        // Bulk load the deleted-keys BTree.
        IIndexBulkLoader deletedKeysBTreeBulkLoader = diskDeletedKeysBTree.createBulkLoader(1.0f, false);
        try {
            while (deletedKeysScanCursor.hasNext()) {
                deletedKeysScanCursor.next();
                deletedKeysBTreeBulkLoader.add(deletedKeysScanCursor.getTuple());
            }
        } finally {
            deletedKeysScanCursor.close();
        }
        deletedKeysBTreeBulkLoader.end();

        return new LSMInvertedIndexComponent(diskInvertedIndex, diskDeletedKeysBTree);
    }

    public void addFlushedComponent(Object index) {
        diskComponents.addFirst(index);
    }

    @Override
    public InMemoryFreePageManager getInMemoryFreePageManager() {
        return memFreePageManager;
    }

    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        memFreePageManager.reset();
        memComponent.getInvIndex().clear();
        memComponent.getDeletedKeysBTree().clear();
    }

    @Override
    public List<Object> getDiskComponents() {
        return diskComponents;
    }

    @Override
    public ILSMComponentFinalizer getComponentFinalizer() {
        return componentFinalizer;
    }
    
    @Override
    public ILSMFlushController getFlushController() {
        return lsmHarness.getFlushController();
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return lsmHarness.getOperationTracker();
    }

    @Override
    public ILSMIOOperationScheduler getIOScheduler() {
        return lsmHarness.getIOScheduler();
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.INVERTED;
    }
}