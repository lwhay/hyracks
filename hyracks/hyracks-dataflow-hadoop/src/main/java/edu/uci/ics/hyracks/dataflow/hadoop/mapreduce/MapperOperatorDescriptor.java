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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapred.DefaultTaskController;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.JobLocalizer;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
//import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
//import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.MRContextUtil;
import edu.uci.ics.hyracks.dataflow.hadoop.util.ResourceLocalizer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import edu.uci.ics.hyracks.hdfs.ContextFactory;

public class MapperOperatorDescriptor<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
        extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(MapperOperatorDescriptor.class.getName());

    private final int jobId;
    private final MarshalledWritable<Configuration> config;
    private final IInputSplitProviderFactory factory;
    private boolean isNewAPI = true;

    public MapperOperatorDescriptor(IOperatorDescriptorRegistry spec, int jobId,
            MarshalledWritable<Configuration> config, IInputSplitProviderFactory factory) throws HyracksDataException {
        super(spec, 0, 1);
        JobSpecification sp = (JobSpecification) spec;
        this.isNewAPI = sp.getProperty("isNewAPI").toString() == "true";
        this.jobId = jobId;
        this.config = config;
        this.factory = factory;
        
        HadoopHelper helper = new HadoopHelper(config);
        recordDescriptors[0] = helper.getMapOutputRecordDescriptor();
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
            throws HyracksDataException {
        final HadoopHelper helper = new HadoopHelper(config);
        final Configuration conf = helper.getConfiguration();
        final String date = new SimpleDateFormat("YYYYMMddHHmm").format(new Date());

        final Object mapper;
        if (this.isNewAPI)
            mapper = helper.getMapper(ctx.getJobletContext().getClassLoader());
        else
            mapper = helper.getOldMapper(ctx.getJobletContext().getClassLoader());
        //        final Mapper<K1, V1, K2, V2> mapper = helper.getMapper(ctx.getJobletContext().getClassLoader()); ////////////////////

        final Object inputFormat;
        if (this.isNewAPI)
            inputFormat = helper.getInputFormat(ctx.getJobletContext().getClassLoader());
        else
            inputFormat = helper.getOldInputFormat(ctx.getJobletContext().getClassLoader());
        //        final InputFormat<K1, V1> inputFormat = helper.getInputFormat(ctx.getJobletContext().getClassLoader()); //////////////////

        final IInputSplitProvider isp = factory.createInputSplitProvider(partition);
        final TaskAttemptID taId = new TaskAttemptID(date, jobId, TaskType.MAP, partition, 0);
        final TaskAttemptContext taskAttemptContext = new ContextFactory().createContext(conf, taId);//helper.createTaskAttemptContext(taId);

        final int framesLimit = helper.getSortFrameLimit(ctx);
        final IBinaryComparatorFactory[] comparatorFactories = helper.getSortComparatorFactories(ctx.getJobletContext()
                .getClassLoader());
//System.out.println("[" + Thread.currentThread().getId() + "][MapperOperatorDescriptor] partition: " + partition + ", nPartitions: " + nPartitions);
//System.out.println("[" + Thread.currentThread().getId() + "][MapperOperatorDescriptor] framesLimit: " + framesLimit + " frames");

        if ((conf.get(DistributedCache.CACHE_FILES) != null) || (conf.get(MRJobConfig.CACHE_FILES) != null)) {
            try {
//                if( (conf.get(DistributedCache.CACHE_FILES) != null) && (conf.get(DistributedCache.CACHE_LOCALFILES) == null) )
//                    conf.set(DistributedCache.CACHE_LOCALFILES, conf.get(DistributedCache.CACHE_FILES));
//                if( (conf.get(MRJobConfig.CACHE_FILES) != null) && (conf.get(MRJobConfig.CACHE_LOCALFILES) == null) )
//                    conf.set(MRJobConfig.CACHE_LOCALFILES, conf.get(MRJobConfig.CACHE_FILES));
                
                setupDistCache(conf);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        class SortingRecordWriter extends RecordWriter<K2, V2> {
            private final ArrayTupleBuilder tb;
            private final ByteBuffer frame;
            private final FrameTupleAppender fta;
            private ExternalSortRunGenerator runGen;
            private int blockId;

            public SortingRecordWriter() throws HyracksDataException {
                tb = new ArrayTupleBuilder(2);
                frame = ctx.allocateFrame();
                fta = new FrameTupleAppender(ctx.getFrameSize());
                fta.reset(frame, true);
            }

            public void initBlock(int blockId) throws HyracksDataException {
                runGen = new ExternalSortRunGenerator(ctx, new int[] { 0 }, null, comparatorFactories,
                        helper.getMapOutputRecordDescriptorWithoutExtraFields(ctx.getJobletContext().getClassLoader()),
                        Algorithm.MERGE_SORT, framesLimit);
                this.blockId = blockId;
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
//                tb.getFieldData().getDataOutputStream().close();
//                System.gc();
            }

            @Override
            public void write(K2 key, V2 value) throws IOException, InterruptedException { // called by custom mapper function
/// DEBUG PRINT ///                
//if(key == null && value == null)
//    System.out.println("[" + Thread.currentThread().getId() + "][SortingRecordWriter][write][both null case] key: null, value: null");
//else{
//    if(key == null)
//        System.out.println("[" + Thread.currentThread().getId() + "][SortingRecordWriter][write][key == null] key: null" + ", value: " + value.toString() + ", valueClass: " + value.getClass());
//    else if(value == null)
//        System.out.println("[" + Thread.currentThread().getId() + "][SortingRecordWriter][write][value == null] key: " + key.toString() + ", keyClass: " + key.getClass() + ", value: null");
//    else
//        System.out.println("[" + Thread.currentThread().getId() + "][SortingRecordWriter][write] key: " + key.toString() + ", keyClass: " + key.getClass() + ", value: " + value.toString() + ", valueClass: " + value.getClass());
//}
            
                DataOutput dos = tb.getDataOutput();
                tb.reset();
//                if (key == null)
//                    key = (K2) HadoopTools.createInstance(helper.getMapOutputKeyClass(ctx.getJobletContext().getClassLoader()));
//                key.write(dos);
//                tb.addFieldEndOffset();
//                
//                if (value == null)
//                    value = (V2) HadoopTools.createInstance(helper.getMapOutputValueClass(ctx.getJobletContext().getClassLoader()));
//                value.write(dos);
//                tb.addFieldEndOffset();
                
                if (key == null)
                    ( (K2) HadoopTools.createInstance(helper.getMapOutputKeyClass(ctx.getJobletContext().getClassLoader())) ).write(dos);
                else 
                    key.write(dos);
                tb.addFieldEndOffset();
                
                if (value == null)
                    ( (V2) HadoopTools.createInstance(helper.getMapOutputValueClass(ctx.getJobletContext().getClassLoader())) ).write(dos);
                else
                    value.write(dos);
                tb.addFieldEndOffset();
                
                if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    runGen.nextFrame(frame);
                    fta.reset(frame, true);
                    if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size ("
                                + frame.capacity() + ")");
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
                    private final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                    private final ByteBuffer outFrame = ctx.allocateFrame();
                    private final FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(),
                            helper.getMapOutputRecordDescriptorWithoutExtraFields(ctx.getJobletContext()
                                    .getClassLoader()));
                    private final ArrayTupleBuilder tb = new ArrayTupleBuilder(3);

                    @Override
                    public void open() throws HyracksDataException {
                        appender.reset(outFrame, true);
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        fta.reset(buffer);
                        //System.out.println("[MapperOperatorDescriptor][delegatingWriter][nextFrame] before FTA prettyPrint");
                        //fta.prettyPrint();
                        //System.out.println("[MapperOperatorDescriptor][delegatingWriter][nextFrame] after FTA prettyPrint");                        
                        int n = fta.getTupleCount();
                        for (int i = 0; i < n; ++i) {
                            tb.reset();
                            tb.addField(fta, i, 0);
                            tb.addField(fta, i, 1);
                            try {
                                tb.getDataOutput().writeInt(blockId);
                            } catch (IOException e) {
                                throw new HyracksDataException(e);
                            }
                            tb.addFieldEndOffset();

                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                FrameUtils.flushFrame(outFrame, writer);
                                appender.reset(outFrame, true);
                                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                    throw new IllegalStateException();
                                }
                            }
                        }
                        //final FrameTupleAccessor frtuac = new FrameTupleAccessor(ctx.getFrameSize(), helper.getMapOutputRecordDescriptor(ctx.getJobletContext()
                        //                .getClassLoader()));
                        //frtuac.reset(appender.getBuffer());
                        //System.out.println("[MapperOperatorDescriptor][delegatingWriter][nextFrame] before FRTUAC prettyPrint");
                        //frtuac.prettyPrint();
                        //System.out.println("[MapperOperatorDescriptor][delegatingWriter][nextFrame] after FRTUAC prettyPrint");
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        if (appender.getTupleCount() > 0) {
                            FrameUtils.flushFrame(outFrame, writer);
                        }
                        System.gc();
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                        // TODO Auto-generated method stub

                    }
                };
                if (helper.hasCombiner(ctx.getJobletContext().getClassLoader())) {
                    Object combiner = null;
                    if (isNewAPI)
                        combiner = helper.getCombiner(ctx.getJobletContext().getClassLoader());
                    else
                        combiner = helper.getOldCombiner(ctx.getJobletContext().getClassLoader());
//                    Reducer<K2, V2, K2, V2> combiner = helper.getCombiner(ctx.getJobletContext().getClassLoader());
                    
//                    TaskAttemptID ctaId = new TaskAttemptID(date, jobId, TaskType.MAP, partition, 0);
                    // TaskAttemptID ctaId = new TaskAttemptID("foo", jobId, true, partition, 0);
//                    TaskAttemptContext ctaskAttemptContext = new ContextFactory().createContext(conf, taId);//helper.createTaskAttemptContext(ctaId);
                    final IFrameWriter outputWriter = delegatingWriter;
                    RecordWriter<K2, V2> recordWriter = new RecordWriter<K2, V2>() {
                        private final FrameTupleAppender fta = new FrameTupleAppender(ctx.getFrameSize());
                        private final ByteBuffer buffer = ctx.allocateFrame();
                        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(2);

                        {
                            fta.reset(buffer, true);
                            outputWriter.open();
                        }

                        @Override
                        public void write(K2 key, V2 value) throws IOException, InterruptedException {
                            DataOutput dos = tb.getDataOutput();
                            tb.reset();
                            if (key == null)
                                key = (K2) HadoopTools.createInstance(helper.getMapOutputKeyClass(ctx.getJobletContext().getClassLoader()));
                            key.write(dos);
                            tb.addFieldEndOffset();
                            
                            if (value == null)
                                value = (V2) HadoopTools.createInstance(helper.getMapOutputValueClass(ctx.getJobletContext().getClassLoader()));
                            value.write(dos);
                            tb.addFieldEndOffset();
                            
                            if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                FrameUtils.flushFrame(buffer, outputWriter);
                                fta.reset(buffer, true);
                                if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                    throw new IllegalStateException();
                                }
                            }
                        }

                        @Override
                        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                            if (fta.getTupleCount() > 0) {
                                FrameUtils.flushFrame(buffer, outputWriter);
                                outputWriter.close();
                            }
                            System.gc();
                        }
                    };
                    if (isNewAPI)
                        delegatingWriter = new ReduceWriter<K2, V2, K2, V2>(ctx, helper,
                                new int[] { HadoopHelper.KEY_FIELD_INDEX }, helper.getGroupingComparatorFactories(),
                                helper.getMapOutputRecordDescriptorWithoutExtraFields(ctx.getJobletContext()
                                        .getClassLoader()), combiner, recordWriter, taId, taskAttemptContext);
                    else{
//                      org.apache.hadoop.mapred.TaskAttemptID otaid = new org.apache.hadoop.mapred.TaskAttemptID(date, jobId, false, partition, 0);
//                        org.apache.hadoop.mapred.TaskAttemptContext otac = org.apache.hadoop.mapred.TaskAttempContext;
                        
                        delegatingWriter = new ReduceWriter<K2, V2, K2, V2>(ctx, helper,
                                new int[] { HadoopHelper.KEY_FIELD_INDEX }, helper.getGroupingComparatorFactories(),
                                helper.getMapOutputRecordDescriptorWithoutExtraFields(ctx.getJobletContext()
                                        .getClassLoader()), combiner, recordWriter, new org.apache.hadoop.mapred.TaskAttemptID(date, jobId, false, partition, 0), taskAttemptContext);
                    }
                }

                IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                for (int i = 0; i < comparatorFactories.length; ++i) {
                    comparators[i] = comparatorFactories[i].createBinaryComparator();
                }
                ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, runGen.getFrameSorter(),
                        runGen.getRuns(), new int[] { 0 }, comparators, null,
                        helper.getMapOutputRecordDescriptorWithoutExtraFields(ctx.getJobletContext().getClassLoader()),
                        framesLimit, delegatingWriter);
                merger.process();
            }
        }

        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                writer.open();
                try {
                    final SortingRecordWriter recordWriter = new SortingRecordWriter();
//                    final Object recordWriter;
//                    if (isNewAPI)
//                        recordWriter = new SortingRecordWriter();
//                    else
//                        recordWriter = new SortingOldRecordWriter();                  
                    
                    Object split = null;
                    int blockId = 0;
                    Object recordReader = null;
                    recordWriter.initBlock(blockId);
//                    if (isNewAPI)
//                        ((SortingRecordWriter) recordWriter).initBlock(blockId);
//                    else
//                        ((SortingOldRecordWriter) recordWriter).initBlock(blockId);

                    ClassLoader ctxCL = Thread.currentThread().getContextClassLoader(); // AppClassLoader
                    Thread.currentThread().setContextClassLoader(ctx.getJobletContext().getClassLoader()); // URLClassLoader
                    if (isNewAPI) { // new api
                        if ((split = isp.get()) != null) {
                            Thread.currentThread().setContextClassLoader(ctxCL);
                            try {
/// DEBUG PRINT ///                                
//Object recordReader2 = ((InputFormat<K1, V1>) inputFormat).createRecordReader(
//        (InputSplit) split, taskAttemptContext);///////////////////////////////
//((RecordReader<K1, V1>) recordReader2)
//        .initialize((InputSplit) split, taskAttemptContext);
//
//StatusReporter sr2 = DatatypeHelper.createDummyStatusReporter();
//                                
//Mapper<K1, V1, K2, V2>.Context tmpmCtx = new MRContextUtil()
//.createMapContext(conf, taId, (RecordReader<K1, V1>) recordReader2,
//        (SortingRecordWriter) recordWriter, null, sr2, (InputSplit) split);
//                                
//while (tmpmCtx.nextKeyValue()) {
//    System.out.println("[" + Thread.currentThread().getId() + "][MapperOperatorDescriptor][newAPI][inputs to map()]key: " + tmpmCtx.getCurrentKey() + ", value: " + tmpmCtx.getCurrentValue());
//  }

                                //RecordReader<K1, V1> recordReader = null;
                                recordReader = ((InputFormat<K1, V1>) inputFormat).createRecordReader(
                                      (InputSplit) split, taskAttemptContext);///////////////////////////////
                                ((RecordReader<K1, V1>) recordReader)
                                      .initialize((InputSplit) split, taskAttemptContext);
                                
                                //                                recordWriter.initBlock(blockId);
                                StatusReporter sr = DatatypeHelper.createDummyStatusReporter();

                                Mapper<K1, V1, K2, V2>.Context mCtx = new MRContextUtil()
                                        .createMapContext(conf, taId, (RecordReader<K1, V1>) recordReader,
                                                (SortingRecordWriter) recordWriter, null, sr, (InputSplit) split);
                                ((Mapper<K1, V1, K2, V2>) mapper).run(mCtx);

                                ((RecordReader<K1, V1>) recordReader).close();
                                ((SortingRecordWriter) recordWriter).sortAndFlushBlock(writer);
                                //                            ++blockId;
                            } catch (IOException e) {
                                throw new HyracksDataException(e);
                            } catch (InterruptedException e) {
                                throw new HyracksDataException(e);
                            }
                        }
                    } else { // old api
                        OutputCollector<K2, V2> collector = new OutputCollector<K2, V2>() {
                            public void collect(K2 k, V2 v) throws IOException {
/// DEBUG PRINT ///                                
//System.out.println("[MapperOperatorDescriptor][oldAPI][collect] Key: " + k);
//System.out.println("[MapperOperatorDescriptor][oldAPI][collect] Value: " + v);
                                try {
                                    recordWriter.write(k, v);
                                } catch (InterruptedException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }

                                DatatypeHelper.createDummyReporter().progress();
                            }
                        };

                        if ((split = isp.getOld()) != null) {
                            try {
                                ((org.apache.hadoop.mapred.Mapper) mapper).configure((JobConf)helper.getConfiguration()); // have to call configure() function of mapper.
                                recordReader = ((org.apache.hadoop.mapred.InputFormat<K1, V1>) inputFormat)
                                        .getRecordReader((org.apache.hadoop.mapred.InputSplit) split,
                                                helper.getJobConf(), DatatypeHelper.createDummyReporter());
                                K1 key = ((org.apache.hadoop.mapred.RecordReader<K1, V1>) recordReader).createKey();
                                V1 value = ((org.apache.hadoop.mapred.RecordReader<K1, V1>) recordReader).createValue();

                                while (((org.apache.hadoop.mapred.RecordReader<K1, V1>) recordReader).next(key, value)) {
/// DEBUG PRINT ///
//System.out.println("[MapperOperatorDescriptor][oldAPI][call_mapper's_map] Key: " + key);
//System.out.println("[MapperOperatorDescriptor][oldAPI][call_mapper's_map] Value: " + value);
                                    // map pair to output
                                    ((org.apache.hadoop.mapred.Mapper) mapper).map(key, value, collector,
                                            DatatypeHelper.createDummyReporter());
                                }
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            } finally {
                                try {
                                    ((org.apache.hadoop.mapred.Mapper) mapper).close();
                                } catch (IOException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }

                            recordWriter.sortAndFlushBlock(writer);
                        }
                    }
                } finally {
                    writer.close();
                }
            }
        };
    }

    private void setupDistCache(Configuration config) throws IOException {
        ResourceLocalizer rl = new ResourceLocalizer(config);
        rl.start();
    }
}