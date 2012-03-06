package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.utils;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.InMemoryBtreeInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.InvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.inverteredindex.LSMInvertedIndexTestHarness;

public class InvertedIndexTestUtils {
    public static InvertedIndex createTestInvertedIndex(LSMInvertedIndexTestHarness harness, IBinaryTokenizer tokenizer) {
//        IInvertedListBuilder builder = new FixedSizeElementInvertedListBuilder(harness.getInvertedListTypeTraits());
//        InvertedIndexFactory factory = new InvertedIndexFactory<IInvertedIndex>(harness.getDiskBufferCache(), harness.getInvertedListTypeTraits(), harness.getInvertedListBinaryComparatorFactories(), builder, tokenizer, harness.get)
//        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) };
//        IFreePageManager diskFreePageManager = new LinkedListFreePageManager(harness.getDiskBufferCache(), headPage, metaDataFrameFactory)
//        BTree btree = new BTree(harness.getDiskBufferCache(), 1, cmpFactories, harness.get, interiorFrameFactory, leafFrameFactory)
//        return LSMInvertedIndexUtils.createInvertedIndex(harness.getDiskBufferCache(), harness., invListFields, invListCmpFactories, tokenizer)
        return null;
    }

    public static InMemoryBtreeInvertedIndex createTestInMemoryBTreeInvertedIndex(LSMInvertedIndexTestHarness harness,
            IBinaryTokenizer tokenizer) {
        return LSMInvertedIndexUtils.createInMemoryBTreeInvertedindex(harness.getMemBufferCache(),
                harness.getMemFreePageManager(), harness.getTokenTypeTraits(), harness.getInvertedListTypeTraits(),
                harness.getTokenBinaryComparatorFactories(), harness.getInvertedListBinaryComparatorFactories(),
                tokenizer);
    }

    public static LSMInvertedIndex createTestLSMInvertedIndex() {
        return null;
    }
}
