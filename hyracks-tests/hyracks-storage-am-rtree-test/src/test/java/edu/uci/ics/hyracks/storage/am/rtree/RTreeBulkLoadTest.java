/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeBulkLoadTest extends AbstractRTreeTest {
    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 10;
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    @Test
    public void test01() throws Exception {

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare keys
        int keyFieldCount = 4;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY).createBinaryComparator();
        cmps[1] = cmps[0];
        cmps[2] = cmps[0];
        cmps[3] = cmps[0];

        // declare tuple fields
        int fieldCount = 7;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = DoublePointable.TYPE_TRAITS;
        typeTraits[1] = DoublePointable.TYPE_TRAITS;
        typeTraits[2] = DoublePointable.TYPE_TRAITS;
        typeTraits[3] = DoublePointable.TYPE_TRAITS;
        typeTraits[4] = IntegerPointable.TYPE_TRAITS;
        typeTraits[5] = IntegerPointable.TYPE_TRAITS;
        typeTraits[6] = DoublePointable.TYPE_TRAITS;

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                cmps.length, DoublePointable.FACTORY);

        MultiComparator cmp = new MultiComparator(cmps);

        RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(typeTraits);

        ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(tupleWriterFactory,
                valueProviderFactories);
        RTreeNSMLeafFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(tupleWriterFactory,
                valueProviderFactories);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);

        RTree rtree = new RTree(bufferCache, fieldCount, cmp, freePageManager, interiorFrameFactory, leafFrameFactory);
        rtree.create(fileId);
        rtree.open(fileId);

        ByteBuffer hyracksFrame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] recDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(hyracksFrame);
        FrameTupleReference tuple = new FrameTupleReference();

        RTree.RTreeBulkLoader loader = rtree.new RTreeBulkLoader();

        Random rnd = new Random();
        rnd.setSeed(50);

        Random rnd2 = new Random();
        rnd2.setSeed(50);
        for (int i = 0; i < 500; i++) {

            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();

            int pk1 = rnd2.nextInt();
            int pk2 = rnd2.nextInt();
            double pk3 = rnd2.nextDouble();

            tb.reset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p1y, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2x, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(p2y, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk1, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
            tb.addFieldEndOffset();
            DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
            tb.addFieldEndOffset();

            appender.reset(hyracksFrame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            try {
                loader.add(tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        loader.build();

        IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory.createFrame();
        IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();
        System.out.println("+++");
        rtree.printTree(leafFrame, interiorFrame, recDescSers);
        System.out.println("+++");

        rtree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();

    }
}
