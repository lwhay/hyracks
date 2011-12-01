package edu.uci.ics.hyracks.storage.am.btree.perf;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public class ConcurrentSkipListRunner implements IExperimentRunner {
    public class TupleComparator implements Comparator<ITupleReference> {
        private final MultiComparator cmp;

        public TupleComparator(MultiComparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(ITupleReference o1, ITupleReference o2) {
            return cmp.compare(o1, o2);
        }
    }
    
    public class TupleCopies {
        public final int numTuples;
        public final TypeAwareTupleWriterFactory tupleWriterFactory;
        public final TypeAwareTupleWriter tupleWriter;
        public final TypeAwareTupleReference[] tuples;
        public final ByteBuffer[] bufs;
        
        public TupleCopies(int numTuples, int tupleSize, ITypeTrait[] typeTraits) {
            this.numTuples = numTuples;
            tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
            tupleWriter = (TypeAwareTupleWriter) tupleWriterFactory.createTupleWriter();
            tuples = new TypeAwareTupleReference[numTuples];
            bufs = new ByteBuffer[numTuples];
            for (int i = 0; i < numTuples; i++) {                
                tuples[i] = (TypeAwareTupleReference) tupleWriter.createTupleReference();
                bufs[i] = ByteBuffer.wrap(new byte[tupleSize]);
            }
        }
    }
    
    private final TupleCopies tupleCopies;
    private final TupleComparator tupleCmp;
    private final int numTuples;
    
    public ConcurrentSkipListRunner(int numTuples, int tupleSize, ITypeTrait[] typeTraits, MultiComparator cmp) {
        this.numTuples = numTuples;
        tupleCopies = new TupleCopies(numTuples, tupleSize, typeTraits);
        tupleCmp = new TupleComparator(cmp);
    }
    
    @Override
    public long runExperiment(DataGenThread dataGen) throws InterruptedException {
        long start = System.currentTimeMillis();
        ConcurrentSkipListSet<ITupleReference> skipList = new ConcurrentSkipListSet<ITupleReference>(tupleCmp);
        for (int i = 0; i < numTuples; i++) {
            ITupleReference tuple = dataGen.tupleQueue.take();
            // Do a "dummy" write to be fair to the BTree.
            tupleCopies.tupleWriter.writeTuple(tuple, tupleCopies.bufs[i].array(), 0);
            tupleCopies.tuples[i].resetByTupleOffset(tupleCopies.bufs[i], 0);
            skipList.add(tupleCopies.tuples[i]);
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
    }
}
