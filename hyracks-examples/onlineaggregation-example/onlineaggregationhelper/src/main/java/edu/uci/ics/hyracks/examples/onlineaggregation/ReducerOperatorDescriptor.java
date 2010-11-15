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
package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class ReducerOperatorDescriptor<K2 extends Writable, V2 extends Writable, K3 extends Writable, V3 extends Writable>
        extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private MarshalledWritable<Configuration> mConfig;

    public ReducerOperatorDescriptor(JobSpecification spec, MarshalledWritable<Configuration> mConfig) {
        super(spec, 1, 0);
        this.mConfig = mConfig;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final HadoopHelper helper = new HadoopHelper(mConfig);
        final Reducer<K2, V2, K3, V3> reducer = helper.getReducer();
        final RecordDescriptor recordDescriptor = helper.getMapOutputRecordDescriptor();
        final FrameTupleAccessor accessor0 = new FrameTupleAccessor(ctx, recordDescriptor);
        final FrameTupleAccessor accessor1 = new FrameTupleAccessor(ctx, recordDescriptor);
        final ByteBuffer copyFrame = ctx.getResourceManager().allocateFrame();
        accessor1.reset(copyFrame);
        final int[] groupFields = helper.getSortFields();
        IBinaryComparatorFactory[] groupingComparators = helper.getGroupingComparatorFactories();
        final IBinaryComparator[] comparators = new IBinaryComparator[groupingComparators.length];
        for (int i = 0; i < comparators.length; ++i) {
            comparators[i] = groupingComparators[i].createBinaryComparator();
        }

        class KVIterator implements RawKeyValueIterator {
            private FrameTupleAccessor accessor;
            private DataInputBuffer kBuffer;
            private DataInputBuffer vBuffer;
            private List<ByteBuffer> buffers;
            private int bSize;
            private int bPtr;
            private int tIdx;
            private boolean eog;

            public KVIterator() {
                accessor = new FrameTupleAccessor(ctx, recordDescriptor);
                kBuffer = new DataInputBuffer();
                vBuffer = new DataInputBuffer();
            }

            void reset(List<ByteBuffer> buffers, int bSize) {
                this.buffers = buffers;
                this.bSize = bSize;
                bPtr = 0;
                tIdx = 0;
                eog = false;
                if (bSize > 0) {
                    accessor.reset(buffers.get(0));
                    tIdx = -1;
                } else {
                    eog = true;
                }
            }

            @Override
            public DataInputBuffer getKey() throws IOException {
                return kBuffer;
            }

            @Override
            public DataInputBuffer getValue() throws IOException {
                return vBuffer;
            }

            @Override
            public boolean next() throws IOException {
                while (true) {
                    if (eog) {
                        return false;
                    }
                    ++tIdx;
                    if (accessor.getTupleCount() <= tIdx) {
                        ++bPtr;
                        if (bPtr >= bSize) {
                            eog = true;
                            continue;
                        }
                        tIdx = -1;
                        accessor.reset(buffers.get(bPtr));
                        continue;
                    }
                    kBuffer.reset(accessor.getBuffer().array(),
                            FrameUtils.getAbsoluteFieldStartOffset(accessor, tIdx, helper.KEY_FIELD_INDEX),
                            accessor.getFieldLength(tIdx, helper.KEY_FIELD_INDEX));
                    vBuffer.reset(accessor.getBuffer().array(),
                            FrameUtils.getAbsoluteFieldStartOffset(accessor, tIdx, helper.VALUE_FIELD_INDEX),
                            accessor.getFieldLength(tIdx, helper.VALUE_FIELD_INDEX));
                    break;
                }
                return true;
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public Progress getProgress() {
                return null;
            }
        }

        final KVIterator kvi = new KVIterator();
        final TaskAttemptID taId = new TaskAttemptID("foo", 0, false, 0, 0);
        final TaskAttemptContext taskAttemptContext = helper.createTaskAttemptContext(taId);
        final RecordWriter recordWriter;
        try {
            recordWriter = helper.getOutputFormat().getRecordWriter(taskAttemptContext);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private boolean first;
            private boolean groupStarted;
            private List<ByteBuffer> group;
            private int bPtr;
            private FrameTupleAppender fta;
            private Counter keyCounter;
            private Counter valueCounter;

            @Override
            public void open() throws HyracksDataException {
                first = true;
                groupStarted = false;
                group = new ArrayList<ByteBuffer>();
                bPtr = 0;
                group.add(ctx.getResourceManager().allocateFrame());
                fta = new FrameTupleAppender(ctx);
                keyCounter = new Counter() {
                };
                valueCounter = new Counter() {
                };
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor0.reset(buffer);
                int nTuples = accessor0.getTupleCount();
                for (int i = 0; i < nTuples; ++i) {
                    if (first) {
                        groupInit();
                        first = false;
                    } else {
                        if (i == 0) {
                            switchGroupIfRequired(accessor1, accessor1.getTupleCount() - 1, accessor0, i);
                        } else {
                            switchGroupIfRequired(accessor0, i - 1, accessor0, i);
                        }
                    }
                    accumulate(accessor0, i);
                }
                FrameUtils.copy(buffer, copyFrame);
            }

            private void accumulate(FrameTupleAccessor accessor, int tIndex) {
                if (!fta.append(accessor, tIndex)) {
                    ++bPtr;
                    if (group.size() <= bPtr) {
                        group.add(ctx.getResourceManager().allocateFrame());
                    }
                    fta.reset(group.get(bPtr), true);
                    if (!fta.append(accessor, tIndex)) {
                        throw new IllegalStateException();
                    }
                }
            }

            private void switchGroupIfRequired(FrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
                    FrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
                if (!sameGroup(prevTupleAccessor, prevTupleIndex, currTupleAccessor, currTupleIndex)) {
                    reduce();
                    groupInit();
                }
            }

            private void groupInit() {
                groupStarted = true;
                bPtr = 0;
                fta.reset(group.get(0), true);
            }

            private void reduce() throws HyracksDataException {
                kvi.reset(group, bPtr + 1);
                try {
                    Reducer<K2, V2, K3, V3>.Context rCtx = reducer.new Context(helper.getConfiguration(), taId, kvi,
                            keyCounter, valueCounter, recordWriter, null, null,
                            (RawComparator<K2>) helper.getRawGroupingComparator(), (Class<K2>) helper.getJob()
                                    .getMapOutputKeyClass(), (Class<V2>) helper.getJob().getMapOutputValueClass());
                    reducer.run(rCtx);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
                groupStarted = false;
            }

            private boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
                for (int i = 0; i < comparators.length; ++i) {
                    int fIdx = groupFields[i];
                    int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength()
                            + a1.getFieldStartOffset(t1Idx, fIdx);
                    int l1 = a1.getFieldLength(t1Idx, fIdx);
                    int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength()
                            + a2.getFieldStartOffset(t2Idx, fIdx);
                    int l2 = a2.getFieldLength(t2Idx, fIdx);
                    if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void flush() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
                if (groupStarted) {
                    reduce();
                }
            }
        };
    }
}