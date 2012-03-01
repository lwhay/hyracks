package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class LSMInvertedIndex implements ILSMIndex {

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

    @Override
    public IIndexAccessor createAccessor() {
        // TODO Auto-generated method stub
        return null;
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

    @Override
    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            IndexException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub
        
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addFlushedComponent(Object index) {
        // TODO Auto-generated method stub
        
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
