package edu.uci.ics.hyracks.storage.am.btree.perf;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeBulkLoadRunner extends BTreeRunner {

    protected final float fillFactor;
    
    public BTreeBulkLoadRunner(int numTuples, int pageSize, int numPages, ITypeTrait[] typeTraits, MultiComparator cmp, float fillFactor)
            throws HyracksDataException, BTreeException {
        super(numTuples, pageSize, numPages, typeTraits, cmp);
        this.fillFactor = fillFactor;
    }

    @Override
    public long runExperiment(DataGenThread dataGen) throws Exception {
        btree.create(btreeFileId);
        long start = System.currentTimeMillis();
        IIndexBulkLoadContext bulkLoadCtx = btree.beginBulkLoad(1.0f);
        for (int i = 0; i < numTuples; i++) {
            ITupleReference tuple = dataGen.tupleQueue.take();
            btree.bulkLoadAddTuple(tuple, bulkLoadCtx);
        }
        btree.endBulkLoad(bulkLoadCtx);
        long end = System.currentTimeMillis();
        long time = end - start;
        return time;
    }
}
