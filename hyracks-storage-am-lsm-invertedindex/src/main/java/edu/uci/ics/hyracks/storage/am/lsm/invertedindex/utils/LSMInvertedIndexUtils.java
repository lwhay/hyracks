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
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class LSMInvertedIndexUtils {
    public static InMemoryBtreeInvertedIndex createInMemoryBTreeInvertedindex(InMemoryBufferCache memBufferCache,
            InMemoryFreePageManager memFreePageManager, ITypeTraits[] tokenTypeTraits, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryComparatorFactory[] invListCmpFactories,
            IBinaryTokenizer tokenizer) {

        // Create the BTree
        int fieldCount = tokenCmpFactories.length + invListCmpFactories.length;
        IBinaryComparatorFactory[] combinedFactories = concatArrays(tokenCmpFactories, invListCmpFactories);
        ITypeTraits[] combinedTraits = concatArrays(tokenTypeTraits, invListTypeTraits);
        ITreeIndexTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(combinedTraits);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        BTree btree = new BTree(memBufferCache, fieldCount, combinedFactories, memFreePageManager,
                interiorFrameFactory, leafFrameFactory);

        return new InMemoryBtreeInvertedIndex(btree, invListTypeTraits, invListCmpFactories, tokenizer);
    }

    public static InvertedIndex createInvertedIndex(IBufferCache bufferCache, BTree btree, ITypeTraits[] invListFields,
            IBinaryComparatorFactory[] invListCmpFactories, IBinaryTokenizer tokenizer) {
        
        IInvertedListBuilder builder = new FixedSizeElementInvertedListBuilder(invListFields);
        return new InvertedIndex(bufferCache, btree, invListFields, invListCmpFactories, builder, tokenizer);
    }

    public static LSMInvertedIndex createLSMInvertedIndex() {
        return null;
    }

    private static <T> T[] concatArrays(T[] first, T[] last) {
        T[] concatenated = Arrays.copyOf(first, first.length + last.length);
        System.arraycopy(last, 0, concatenated, first.length, last.length);
        return concatenated;
    }
}
