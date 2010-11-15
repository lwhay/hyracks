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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunMerger;

public class MapperOperatorDescriptor<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
        extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final MarshalledWritable<Configuration> config;
    private final IInputSplitProviderFactory isProviderFactory;

    public MapperOperatorDescriptor(JobSpecification spec, MarshalledWritable<Configuration> config,
            IInputSplitProviderFactory isProviderFactory) throws HyracksDataException {
        super(spec, 0, 1);
        this.config = config;
        this.isProviderFactory = isProviderFactory;
        HadoopHelper helper = new HadoopHelper(config);
        recordDescriptors[0] = helper.getMapOutputRecordDescriptor();
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        HadoopHelper helper = new HadoopHelper(config);
        final Configuration conf = helper.getConfiguration();
        final Mapper<K1, V1, K2, V2> mapper = helper.getMapper();
        final InputFormat<K1, V1> inputFormat = helper.getInputFormat();
        final IInputSplitProvider isp = isProviderFactory.createInputSplitProvider();
        final TaskAttemptID taId = new TaskAttemptID("foo", 0, true, 0, 0);
        final TaskAttemptContext taskAttemptContext = helper.createTaskAttemptContext(taId);

        final int framesLimit = helper.getSortFrameLimit(ctx);
        final IBinaryComparatorFactory[] comparatorFactories = helper.getSortComparatorFactories();

        class SortingRecordWriter extends RecordWriter<K2, V2> {
            private final ArrayTupleBuilder tb;
            private final ByteBuffer frame;
            private final FrameTupleAppender fta;
            private ExternalSortRunGenerator runGen;
            private int blockId;

            public SortingRecordWriter() throws HyracksDataException {
                tb = new ArrayTupleBuilder(3);
                frame = ctx.getResourceManager().allocateFrame();
                fta = new FrameTupleAppender(ctx);
                fta.reset(frame, true);
            }

            public void initBlock(int blockId) {
                runGen = new ExternalSortRunGenerator(ctx, new int[] { 0 }, null, comparatorFactories,
                        recordDescriptors[0], framesLimit);
                this.blockId = blockId;
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            }

            @Override
            public void write(K2 key, V2 value) throws IOException, InterruptedException {
                DataOutput dos = tb.getDataOutput();
                tb.reset();
                key.write(dos);
                tb.addFieldEndOffset();
                value.write(dos);
                tb.addFieldEndOffset();
                dos.writeInt(blockId);
                tb.addFieldEndOffset();
                if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    runGen.nextFrame(frame);
                    fta.reset(frame, true);
                    if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        throw new IllegalStateException();
                    }
                }
            }

            public void sortAndFlushBlock(final IFrameWriter writer) throws HyracksDataException {
                if (fta.getTupleCount() > 0) {
                    runGen.nextFrame(frame);
                    fta.reset(frame, true);
                }
                runGen.close();
                IFrameWriter delegatingWriter = new IFrameWriter() {
                    @Override
                    public void open() throws HyracksDataException {
                        // do nothing
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        writer.nextFrame(buffer);
                    }

                    @Override
                    public void flush() throws HyracksDataException {
                        writer.flush();
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        // do nothing
                    }
                };
                ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, runGen.getFrameSorter(),
                        runGen.getRuns(), new int[] { 0 }, comparatorFactories, recordDescriptors[0], framesLimit,
                        delegatingWriter);
                merger.process();
            }
        }

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                writer.open();
                try {
                    SortingRecordWriter recordWriter = new SortingRecordWriter();
                    while (isp.next(partition, nPartitions)) {
                        InputSplit is = isp.getInputSplit();
                        int blockId = isp.getBlockId();
                        try {
                            RecordReader<K1, V1> recordReader = inputFormat.createRecordReader(is, taskAttemptContext);
                            ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
                            try {
                                Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                                recordReader.initialize(is, taskAttemptContext);
                            } finally {
                                Thread.currentThread().setContextClassLoader(ctxCL);
                            }
                            recordWriter.initBlock(blockId);
                            Mapper<K1, V1, K2, V2>.Context mCtx = mapper.new Context(conf, taId, recordReader,
                                    recordWriter, null, null, is);
                            mapper.run(mCtx);
                            recordReader.close();
                            recordWriter.sortAndFlushBlock(writer);
                            writer.flush();
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        } catch (InterruptedException e) {
                            throw new HyracksDataException(e);
                        }
                    }
                } finally {
                    writer.close();
                }
            }
        };
    }
}