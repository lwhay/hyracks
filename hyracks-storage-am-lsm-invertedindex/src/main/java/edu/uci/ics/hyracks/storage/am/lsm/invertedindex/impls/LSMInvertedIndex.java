package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class LSMInvertedIndex implements ILSMTree {

	@Override
	public ITreeIndexAccessor createAccessor() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IIndexBulkLoadContext beginBulkLoad(float fillFactor)
			throws TreeIndexException, HyracksDataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void bulkLoadAddTuple(ITupleReference tuple,
			IIndexBulkLoadContext ictx) throws HyracksDataException {
		// TODO Auto-generated method stub

	}

	@Override
	public void endBulkLoad(IIndexBulkLoadContext ictx)
			throws HyracksDataException {
		// TODO Auto-generated method stub

	}

	@Override
	public ITreeIndexFrameFactory getLeafFrameFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITreeIndexFrameFactory getInteriorFrameFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IFreePageManager getFreePageManager() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getFieldCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getRootPageId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public IndexType getIndexType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getFileId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public IBufferCache getBufferCache() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IBinaryComparatorFactory[] getComparatorFactories() {
		// TODO Auto-generated method stub
		return null;
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

	@Override
	public boolean insertUpdateOrDelete(ITupleReference tuple,
			IIndexOpContext ictx) throws HyracksDataException,
			TreeIndexException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void search(ITreeIndexCursor cursor, List<Object> diskComponents,
			ISearchPredicate pred, IIndexOpContext ictx,
			boolean includeMemComponent, AtomicInteger searcherRefCount)
			throws HyracksDataException, TreeIndexException {
		// TODO Auto-generated method stub

	}

	@Override
	public Object merge(List<Object> mergedComponents)
			throws HyracksDataException, TreeIndexException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addMergedComponent(Object newComponent,
			List<Object> mergedComponents) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanUpAfterMerge(List<Object> mergedComponents)
			throws HyracksDataException {
		// TODO Auto-generated method stub

	}

	@Override
	public Object flush() throws HyracksDataException, TreeIndexException {
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
