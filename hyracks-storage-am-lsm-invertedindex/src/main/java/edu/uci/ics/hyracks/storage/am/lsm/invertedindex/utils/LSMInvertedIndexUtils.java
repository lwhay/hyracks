package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.utils;

import java.util.Arrays;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.InMemoryBtreeInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;

public class LSMInvertedIndexUtils {
    public static InMemoryBtreeInvertedIndex createInMemoryBTreeInvertedindex(InMemoryBufferCache memBufferCache,
            InMemoryFreePageManager memFreePageManager, ITypeTraits[] btreeTypeTraits, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] btreeCmpFactories, IBinaryComparatorFactory[] invListCmpFactories,
            IBinaryTokenizer tokenizer) {
        
        // Create the BTree
        int fieldCount = btreeCmpFactories.length + invListCmpFactories.length;
        IBinaryComparatorFactory[] combinedFactories = concatArrays(btreeCmpFactories, invListCmpFactories);
        ITypeTraits[] combinedTraits = concatArrays(btreeTypeTraits, invListTypeTraits);
        ITreeIndexTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(combinedTraits);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        BTree btree = new BTree(memBufferCache, fieldCount, combinedFactories, memFreePageManager,
                interiorFrameFactory, leafFrameFactory);
        
        return new InMemoryBtreeInvertedIndex(btree, invListTypeTraits, invListCmpFactories, tokenizer);
    }

    private static <T> T[] concatArrays(T[] first, T[] last) {
        T[] concated = Arrays.copyOf(first, first.length + last.length);
        System.arraycopy(last, 0, concated, first.length, last.length);
        return concated;
    }

    public static InvertedIndex createInvertedIndex() {
        return null;
    }

    public static LSMInvertedIndex createLSMInvertedIndex() {
        return null;
    }
}
