package edu.uci.ics.hyracks.storage.am.btree.perf;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BTreeRunner implements IExperimentRunner {
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    
    protected IHyracksTaskContext ctx; 
    protected IBufferCache bufferCache;
    protected int btreeFileId;
    
    protected final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    protected final static String tmpDir = System.getProperty("java.io.tmpdir");
    protected final static String sep = System.getProperty("file.separator");
    protected String fileName;
    
    protected final int numTuples;
    protected final BTree btree;
    protected final ITreeIndexAccessor indexAccessor;
    
    public BTreeRunner(int numTuples, int pageSize, int numPages, ITypeTrait[] typeTraits, MultiComparator cmp) throws HyracksDataException, BTreeException {
        this.numTuples = numTuples;
        fileName = tmpDir + sep + simpleDateFormat.format(new Date());
        ctx = TestUtils.create(HYRACKS_FRAME_SIZE);
        TestStorageManagerComponentHolder.init(pageSize, numPages, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        btreeFileId = fmp.lookupFileId(file);
        bufferCache.openFile(btreeFileId);
        btree = BTreeUtils
                .createBTree(bufferCache, btreeFileId, typeTraits, cmp.getComparators(), BTreeLeafFrameType.REGULAR_NSM);
        indexAccessor = btree.createAccessor();        
    }

    @Override
    public long runExperiment(DataGenThread dataGen) throws Exception {
        btree.create(btreeFileId);
        long start = System.currentTimeMillis();
        for (int i = 0; i < numTuples; i++) {
            ITupleReference tuple = dataGen.tupleQueue.take();
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            } 
        }
        long end = System.currentTimeMillis();
        long time = end - start;
        return time;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public void deinit() throws Exception {
        bufferCache.closeFile(btreeFileId);
        bufferCache.close();
    }
}
