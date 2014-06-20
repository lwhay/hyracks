package edu.uci.ics.hyracks.storage.am.common.impls;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFilePath;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.impls.NodeFrontier;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.buffercache.IDFSBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.StagingPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.BufferedFilePath;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractWriteOnlyIndex implements ITreeIndex {

    protected final IDFSBufferCache bufferCache;

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;

    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int fieldCount;
    protected final IFileMapProvider fileMapProvider;
    protected final ITreeIndexMetaDataFrameFactory metaFactory;
    protected ITreeIndexMetaDataFrame metaFrame;

    private boolean isActivated = false;
    protected int fileId = -1;
    protected IFilePath path;

    public AbstractWriteOnlyIndex(IDFSBufferCache bufferCache, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            IFilePath path, IFileMapProvider fileMapProvider, ITreeIndexMetaDataFrameFactory metaFrameFactory) {
        this.bufferCache = bufferCache;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.fieldCount = fieldCount;
        this.fileMapProvider = fileMapProvider;
        this.path = path;
        this.metaFactory = metaFrameFactory;
    }

    public void create() throws HyracksDataException {
        // TODO Auto-generated method stub
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(path);
            if (!fileIsMapped) {
                bufferCache.createFile(path);
            }
            fileId = fileMapProvider.lookupFileId(path);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }

        initEmptyTree();
        bufferCache.closeFile(fileId);
    }

    private void initEmptyTree() throws HyracksDataException {
        //create the metadata frame... do nothing else
        ITreeIndexFrame frame = leafFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFactory.createFrame();
    }

    @Override
    public void activate() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(path);
            if (!fileIsMapped) {
                throw new HyracksDataException("Index File does not exist; cannot activate.");
            }
            fileId = fileMapProvider.lookupFileId(path);
            try {
                // Also creates the file if it doesn't exist yet.
                //^^is this ok?
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }
        //freePageManager.open(fileId);

        // TODO: Should probably have some way to check that the tree is physically consistent
        // or that the file we just opened actually is a tree

        isActivated = true;
    }

    @Override
    public void clear() throws HyracksDataException {
        //ideally i should be able to delete and recreate the DFS file but this is whatever for now
        throw new HyracksDataException("Invalid Operation");
    }

    @Override
    public void deactivate() throws HyracksDataException {
        // TODO Auto-generated method stub
        if (!isActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }

        bufferCache.closeFile(fileId);
        isActivated = false;

    }

    @Override
    public void destroy() throws HyracksDataException {
        throw new HyracksDataException("Invalid Operation");

    }

    @Override
    public void validate() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    //TODO: think harder about this
    public IBufferCache getBufferCache() {
        if (bufferCache.getClass().equals(BufferCache.class)) {
            return (IBufferCache) bufferCache;
        } else
            return null;
    }

    @Override
    public long getMemoryAllocationSize() {
        return 0l;
    }

    @Override
    public boolean hasMemoryComponents() {
        return false;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return null; //TODO: would be better if i could throw an exception or similar
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public int getRootPageId() {
        //will return the end eventually
        return 0;
    }

    @Override
    public int getFileId() {
        return fileId;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    public abstract class AbstractInOrderBulkLoader implements IIndexBulkLoader {
        protected final MultiComparator cmp;
        protected final int slotSize;
        protected final int leafMaxBytes;
        protected final int interiorMaxBytes;
        protected final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();
        protected final ITreeIndexTupleWriter tupleWriter;
        protected ITreeIndexFrameFactory leafFrameFactory;
        protected ITreeIndexFrameFactory interiorFrameFactory;
        protected ITreeIndexFrame leafFrame;
        protected ITreeIndexFrame interiorFrame;
        private boolean releasedLatches;

        public AbstractInOrderBulkLoader(float fillFactor, ITreeIndexFrameFactory leafFrameFactory,
                ITreeIndexFrameFactory interiorFrameFactory) throws TreeIndexException, HyracksDataException {
            this.interiorFrameFactory = interiorFrameFactory;
            this.leafFrameFactory = leafFrameFactory;
            leafFrame = leafFrameFactory.createFrame();
            interiorFrame = interiorFrameFactory.createFrame();

            /* dont worry about this for now
            if (!isEmptyTree(leafFrame)) {
                throw new TreeIndexException("Cannot bulk-load a non-empty tree.");
            }
            */
            this.cmp = MultiComparator.create(cmpFactories);

            leafFrame.setMultiComparator(cmp);
            interiorFrame.setMultiComparator(cmp);

            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = getFreshPage();
            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId), true);
            leafFrontier.page.acquireWriteLatch();

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) ((float) interiorFrame.getBuffer().capacity() * fillFactor);

            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) ((float) leafFrame.getBuffer().capacity() * fillFactor);
            slotSize = leafFrame.getSlotSize();

            nodeFrontiers.add(leafFrontier);
        }

        public abstract void add(ITupleReference tuple) throws IndexException, HyracksDataException;

        protected void handleException() throws HyracksDataException {
            // Unlatch and unpin pages.
            for (NodeFrontier nodeFrontier : nodeFrontiers) {
                nodeFrontier.page.releaseWriteLatch(true);
                bufferCache.unpin(nodeFrontier.page);
            }
            releasedLatches = true;
        }

        @Override
        public void end() throws HyracksDataException {
            //we must allocate all open node frontiers a on-disk page and put them on it
            if (!releasedLatches) {
                for (int i = 0; i < nodeFrontiers.size(); i++) {
                    NodeFrontier memBranch = nodeFrontiers.get(i);
                    try {
                        nodeFrontiers.get(i).page.releaseWriteLatch(true);
                    } catch (IllegalMonitorStateException e) {
                        //ignore illegal monitor state exception
                    }
                    memBranch.page.acquireReadLatch();
                    int permPage = getFreshPage();
                    ICachedPage interiorPage = bufferCache.pin(BufferedFilePath.getDiskPageId(fileId, permPage), true);
                    System.arraycopy(memBranch.page.getBuffer().array(), 0, interiorPage.getBuffer().array(), 0,
                            memBranch.page.getBuffer().capacity());

                    memBranch.page.releaseReadLatch();

                }
            }
            // finalize the root generated from the bulk-load to the end-1 location 
            int rootPage = getFreshPage();
            ICachedPage newRoot = bufferCache.pin(BufferedFilePath.getDiskPageId(fileId, rootPage), true);
            newRoot.acquireWriteLatch();
            NodeFrontier lastNodeFrontier = nodeFrontiers.get(nodeFrontiers.size() - 1);
            try {
                System.arraycopy(lastNodeFrontier.page.getBuffer().array(), 0, newRoot.getBuffer().array(), 0,
                        lastNodeFrontier.page.getBuffer().capacity());
            } finally {
                newRoot.releaseWriteLatch(true);
                bufferCache.unpin(newRoot);

                // register old root as a free page
                //freePageManager.addFreePage(metaFrame, lastNodeFrontier.pageId);
                //write the metadata page
                int metaData = getFreshPage();
                ICachedPage meta = bufferCache.pin(BufferedFilePath.getDiskPageId(fileId, metaData), true);
                meta.acquireWriteLatch();
                System.arraycopy(metaFrame.getPage().getBuffer().array(), 0, meta.getBuffer().array(), 0, metaFrame
                        .getPage().getBuffer().capacity());
                meta.releaseWriteLatch(true);
            }
        }

        protected int getFreshPage() {
            int tail = metaFrame.getMaxPage();
            ++tail;
            metaFrame.setMaxPage(tail);
            return tail;
        }

        protected void addLevel() throws HyracksDataException {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.pageId = -1; //frontier parents are in-memory strictly
            frontier.page = new StagingPage(leafFrame.getBuffer().capacity());
            frontier.page.acquireWriteLatch();
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }

        public ITreeIndexFrame getLeafFrame() {
            return leafFrame;
        }

        public void setLeafFrame(ITreeIndexFrame leafFrame) {
            this.leafFrame = leafFrame;
        }
    }
}
