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

package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import java.util.ArrayList;
import java.util.Random;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.InMemoryBtreeInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.utils.LSMInvertedIndexUtils;

public abstract class AbstractInvIndexSearchTest extends AbstractInvIndexTest {
    protected LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();

    // Token information
    protected ITypeTraits[] tokenTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS };
    protected IBinaryComparatorFactory[] tokenCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY) };

    // Inverted list information
    protected ITypeTraits[] invListTypeTraits = new ITypeTraits[] { IntegerPointable.TYPE_TRAITS };
    protected IBinaryComparatorFactory[] invListCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(IntegerPointable.FACTORY) };

    protected InMemoryBtreeInvertedIndex invIndex;
    protected IIndexCursor resultCursor;
    protected ITokenFactory tokenFactory;
    protected IBinaryTokenizer tokenizer;    
    
    protected Random rnd = new Random();

    protected ISerializerDeserializer[] querySerde = { UTF8StringSerializerDeserializer.INSTANCE };
    protected ArrayTupleBuilder queryTb = new ArrayTupleBuilder(querySerde.length);
    protected ArrayTupleReference queryTuple = new ArrayTupleReference();

    protected abstract void setTokenizer();

    @Before
    public void start() throws Exception {
        harness.setUp();
        invIndex = LSMInvertedIndexUtils.createInMemoryBTreeInvertedindex(harness.getMemBufferCache(),
                harness.getMemFreePageManager(), tokenTypeTraits, invListTypeTraits, tokenCmpFactories,
                invListCmpFactories, tokenizer);
        invIndex.create(harness.getFileId());
        invIndex.open(harness.getFileId());
    }

    @After
    public void deinit() throws HyracksDataException {
        invIndex.close();
        harness.tearDown();
    }
}
