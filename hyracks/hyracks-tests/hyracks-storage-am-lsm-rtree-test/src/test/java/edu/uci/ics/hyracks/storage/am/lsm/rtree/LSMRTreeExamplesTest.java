/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.rtree;

import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.AbstractRTreeExamplesTest;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

public class LSMRTreeExamplesTest extends AbstractRTreeExamplesTest {
    private final LSMRTreeTestHarness harness = new LSMRTreeTestHarness();

    public LSMRTreeExamplesTest() {
        super();
        this.rTreeType = RTreeType.LSMRTREE;
    }

    @Override
    protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, int[] rtreeFields, int[] btreeFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields) throws TreeIndexException {
        return LSMRTreeUtils.createLSMTree(harness.getVirtualBufferCaches(), harness.getFileReference(),
                harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), typeTraits, rtreeCmpFactories,
                btreeCmpFactories, valueProviderFactories, rtreePolicyType, harness.getBoomFilterFalsePositiveRate(),
                harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallback(),
                LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length), rtreeFields, btreeFields,
                filterTypeTraits, filterCmpFactories, filterFields);
    }

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * Test the LSM component filters.
     */
    @Test
    public void lsmComponentFilteringExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing LSMRTree component filters.");
        }

        // Declare fields.
        int fieldCount = 6;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = IntegerPointable.TYPE_TRAITS;
        typeTraits[5] = IntegerPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 1;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 6;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
            btreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
            btreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
            btreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
            btreeCmpFactories[4] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
            btreeCmpFactories[5] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmpFactories.length, IntegerPointable.FACTORY);

        int[] rtreeFields = { 0, 1, 2, 3, 4 };
        ITypeTraits[] filterTypeTraits = { IntegerPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] filterCmpFactories = { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) };
        int[] filterFields = { 5 };

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, RTreePolicyType.RTREE, rtreeFields, btreeFields, filterTypeTraits,
                filterCmpFactories, filterFields);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            int p1x = rnd.nextInt();
            int p1y = rnd.nextInt();
            int p2x = rnd.nextInt();
            int p2y = rnd.nextInt();

            int pk = 5;
            int filter = i;

            TupleUtils.createIntegerTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                    Math.max(p1y, p2y), pk, filter);
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        scan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(keyTb, key, -1000, -1000, 1000, 1000);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

}
