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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.util.Progressable;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.MRContextUtil;
import edu.uci.ics.hyracks.hdfs.ContextFactory;


public class ReduceWriter<K2, V2, K3, V3> implements IFrameWriter {

    /**
     * This class has been taken from ReduceValuesIterator class in Hadoop 2.2.0 source code with minor modifications.
     */
    private class ReduceValuesIterator<KEY,VALUE> implements Iterator<VALUE> {
        protected RawKeyValueIterator in; //input iterator
        private KEY key;               // current key
        private KEY nextKey;
        private VALUE value;             // current value
        private boolean hasNext;                      // more w/ this key
        private boolean more;                         // more in file
        private RawComparator<KEY> comparator;
        protected Progressable reporter;
        private Deserializer<KEY> keyDeserializer;
        private Deserializer<VALUE> valDeserializer;
        private DataInputBuffer keyIn = new DataInputBuffer();
        private DataInputBuffer valueIn = new DataInputBuffer();
        
        public ReduceValuesIterator (RawKeyValueIterator in,
                                   RawComparator<KEY> comparator, 
                                   Class<KEY> keyClass,
                                   Class<VALUE> valClass,
                                   Configuration conf, Progressable reporter)
        throws IOException {
            this.in = in;
            this.comparator = comparator;
            this.reporter = reporter;
            SerializationFactory serializationFactory = new SerializationFactory(conf);
            this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
            this.keyDeserializer.open(keyIn);
            this.valDeserializer = serializationFactory.getDeserializer(valClass);
            this.valDeserializer.open(this.valueIn);
            readNextKey();
            key = nextKey;
            nextKey = null; // force new instance creation
            hasNext = more;
        }
        
        private int ctr = 0;
        public VALUE next() {
            if (!hasNext) {
                throw new NoSuchElementException("iterate past last value");
            }
            try {
                readNextValue();
                readNextKey();
            } catch (IOException ie) {
                throw new RuntimeException("problem advancing post rec#" + ctr, ie);
            }
            reporter.progress();
            return value;
        }
        
        private void readNextKey() throws IOException {
            more = in.next();
            if (more) {
                DataInputBuffer nextKeyBytes = in.getKey();
                keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(), nextKeyBytes.getLength());
                nextKey = keyDeserializer.deserialize(nextKey);
                hasNext = key != null && (comparator.compare(key, nextKey) == 0);
            } else {
                hasNext = false;
            }
        }
        
        private void readNextValue() throws IOException {
          DataInputBuffer nextValueBytes = in.getValue();
          valueIn.reset(nextValueBytes.getData(), nextValueBytes.getPosition(), nextValueBytes.getLength());
          value = valDeserializer.deserialize(value);
        }
        
        public void nextKey() throws IOException {
            // read until we find a new key
            while (hasNext) { 
              readNextKey();
            }
            ++ctr;
            
            // move the next key to the current one
            KEY tmpKey = key;
            key = nextKey;
            nextKey = tmpKey;
            hasNext = more;
          }
        
        /** True iff more keys remain. */
        public boolean more() { 
          return more; 
        }

        /** The current key. */
        public KEY getKey() { 
          return key; 
        }

        @Override
        public boolean hasNext() { return hasNext; }

        @Override
        public void remove() { throw new RuntimeException("not implemented"); }
    }

    private final IHyracksTaskContext ctx;
    private final HadoopHelper helper;
    private final int[] groupFields;
    private final FrameTupleAccessor accessor0;
    private final FrameTupleAccessor accessor1;
    private final ByteBuffer copyFrame;
    private final IBinaryComparator[] comparators;
    private final KVIterator kvi;
    private final KVIterator kvi2;
    private final/*Reducer<K2, V2, K3, V3>*/Object reducer;
    private final/*RecordWriter<K3, V3>*/Object recordWriter;
    private final/*TaskAttemptID*/Object taId;
    private final/*TaskAttemptContext*/Object taskAttemptContext;

    private boolean first;
//    private boolean closeCalled;
    private boolean groupStarted;
    private List<ByteBuffer> group;
    private int bPtr;
    private FrameTupleAppender fta;
    private Counter keyCounter;
    private Counter valueCounter;
    private boolean isNewAPI = true;
    private Exception exception;

    public ReduceWriter(IHyracksTaskContext ctx, HadoopHelper helper, int[] groupFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor,
            /*Reducer<K2, V2, K3, V3>*/Object reducer, /*RecordWriter<K3, V3>*/Object recordWriter, /*TaskAttemptID*/
            Object taId,
            /*TaskAttemptContext*/Object taskAttemptContext) throws HyracksDataException {
        this.ctx = ctx;
        this.helper = helper;
        this.groupFields = groupFields;
        accessor0 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        accessor1 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        copyFrame = ctx.allocateFrame();
        accessor1.reset(copyFrame);
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.reducer = reducer;
        this.recordWriter = recordWriter;
        this.taId = taId;
        this.taskAttemptContext = taskAttemptContext;
        JobConf jobConf = (JobConf) helper.getConfiguration();
        this.isNewAPI = jobConf.getUseNewReducer();

        kvi = new KVIterator(ctx, helper, recordDescriptor);
        kvi2 = new KVIterator(ctx, helper, recordDescriptor);
    }

    @Override
    public void open() throws HyracksDataException {
        first = true;
//        closeCalled = false;
        groupStarted = false;
        group = new ArrayList<ByteBuffer>();
        bPtr = 0;
        group.add(ctx.allocateFrame());
        fta = new FrameTupleAppender(ctx.getFrameSize());
        keyCounter = DatatypeHelper.createCounter();
        valueCounter = DatatypeHelper.createCounter();

    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor0.reset(buffer);
/// DEBUG PRINT ///        
//System.out.println("[ReduceWriter][nextFrame] before prettyPrint");
//accessor0.prettyPrint();
//System.out.println("[ReduceWriter][nextFrame] after prettyPrint");
        
        int nTuples = accessor0.getTupleCount();
        for (int i = 0; i < nTuples; ++i) {
/// DEBUG PRINT ///            
//System.out.println("[" + Thread.currentThread().getId() + "][ReduceWriter][nextFrame] i: " + i);            
            if (first) {
                groupInit();
                first = false;
            } else {
////////////////////////////////////////////////////////////////////////
//                if (i == 0) {
//                    switchGroupIfRequired(accessor1, accessor1.getTupleCount() - 1, accessor0, i);
//                } else {
//                    switchGroupIfRequired(accessor0, i - 1, accessor0, i);
//                }
////////////////////////////////////////////////////////////////////////                
            }
            accumulate(accessor0, i);
        }
        FrameUtils.copy(buffer, copyFrame);
    }

    private void accumulate(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
        if (!fta.append(accessor, tIndex)) {
            ++bPtr;
            if (group.size() <= bPtr) {
                group.add(ctx.allocateFrame());
            }
            fta.reset(group.get(bPtr), true);
            if (!fta.append(accessor, tIndex)) {
                throw new HyracksDataException("Record size ("
                        + (accessor.getTupleEndOffset(tIndex) - accessor.getTupleStartOffset(tIndex))
                        + ") larger than frame size (" + group.get(bPtr).capacity() + ")");
            }
        }
    }

    private void switchGroupIfRequired(FrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            FrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        if (!sameGroup(prevTupleAccessor, prevTupleIndex, currTupleAccessor, currTupleIndex)) {
/// DEBUG PRINT ///            
//System.out.println("[" + Thread.currentThread().getId() + "][ReduceWriter][switchGroupIfRequired] calling reduce()");
            
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
//        if (!closeCalled)
//            return;    
        
        kvi.reset(group, bPtr + 1);
        kvi2.reset(group, bPtr + 1);
        
        if (this.isNewAPI) {
            ClassLoader jobCL = helper.getJob().getConfiguration().getClassLoader();
            try {
                helper.getJob().getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
                
//System.out.println("[ReduceWriter][reduce] just before calling custom reducer!!");                
//Reducer<K2, V2, K3, V3>.Context tmprCtx = new MRContextUtil().createReduceContext(helper
//        .getConfiguration(), (TaskAttemptID) taId, kvi2, keyCounter, valueCounter,
//        (RecordWriter<K3, V3>) recordWriter, null, DatatypeHelper.createDummyStatusReporter(), (RawComparator<K2>) helper
//                .getRawGroupingComparator(), (Class<K2>) helper.getJob().getMapOutputKeyClass(),
//        (Class<V2>) helper.getJob().getMapOutputValueClass());            
//while (tmprCtx.nextKey()) {
//    System.out.println("[" + Thread.currentThread().getId() + "][ReduceWriter][Call reduce with] currentKey: " + tmprCtx.getCurrentKey() + ", keyClass: " + tmprCtx.getCurrentKey().getClass());
//    Iterator<V2> iter = tmprCtx.getValues().iterator();
//    while(iter.hasNext()){
//        V2 v = iter.next();
//        System.out.println("[" + Thread.currentThread().getId() + "][ReduceWriter][Call reduce with] value: " + v + ", valueClass: " + v.getClass());
//    }
//}
                Reducer<K2, V2, K3, V3>.Context rCtx = new MRContextUtil().createReduceContext(helper
                        .getConfiguration(), (TaskAttemptID) taId, kvi, keyCounter, valueCounter,
                        (RecordWriter<K3, V3>) recordWriter, null, DatatypeHelper.createDummyStatusReporter(), (RawComparator<K2>) helper
                                .getRawGroupingComparator(), (Class<K2>) helper.getJob().getMapOutputKeyClass(),
                        (Class<V2>) helper.getJob().getMapOutputValueClass());
System.out.println("Reducer Class: " + reducer.getClass());                
                ((Reducer<K2, V2, K3, V3>) reducer).run(rCtx);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            } finally {
                helper.getJob().getConfiguration().setClassLoader(jobCL);
            }

        } else {
//            StatusReporter sr = DatatypeHelper.createDummyStatusReporter();
//            ReduceCtx rc = new ReduceCtx(helper.getConfiguration(), (org.apache.hadoop.mapred.TaskAttemptID) taId, kvi,
//                    keyCounter, valueCounter, (org.apache.hadoop.mapred.RecordWriter<K3, V3>) recordWriter, null, sr,
//                    (RawComparator<K2>) helper.getRawGroupingComparator(), (Class<K2>) helper.getJobConf()
//                            .getMapOutputKeyClass(), (Class<V2>) helper.getJobConf().getMapOutputValueClass());
//            
            OutputCollector<K3, V3> collector = new OutputCollector<K3, V3>() {
                public void collect(K3 k, V3 v) throws IOException {
//                    ((org.apache.hadoop.mapred.RecordWriter<K3, V3>) recordWriter).write(k, v);
                    try {
                        System.out.println("=============== key: " + k + ", value: " + v + " ================");
                        ((RecordWriter<K3, V3>) recordWriter).write(k, v);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
//                    System.out.println("%%%%%%%% key: " + k);
//                    String[] a = v.toString().split("");
//                    System.out.println("######### 1: " + a[0] + ", 2: " + a[1]);
                    DatatypeHelper.createDummyReporter().progress();
                }
            };
            
            ClassLoader jobCL = helper.getJob().getConfiguration().getClassLoader();
            try {
                helper.getJob().getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
                ReduceValuesIterator<K2,V2> values = new ReduceValuesIterator<K2,V2>(kvi, 
                        (RawComparator<K2>) new JobConf(helper.getJob().getConfiguration()).getOutputValueGroupingComparator(), (Class<K2>) helper.getJob()
                      .getMapOutputKeyClass(), (Class<V2>) helper.getJob().getMapOutputValueClass(), 
                        helper.getConfiguration(), DatatypeHelper.createDummyReporter());
                
                while(values.more()){
                    ((org.apache.hadoop.mapred.Reducer<K2, V2, K3, V3>) reducer).reduce((K2) values.getKey(),
                          (Iterator<V2>) values, collector, DatatypeHelper.createDummyReporter());
                    values.nextKey();
                }
            } catch (IOException e) {
                throw new HyracksDataException(e);
            } finally {
                helper.getJob().getConfiguration().setClassLoader(jobCL);
            }

//            try {
//                while (rc.nextKey()) {
//                    ((org.apache.hadoop.mapred.Reducer<K2, V2, K3, V3>) reducer).reduce((K2) rc.getCurrentKey(),
//                            (Iterator<V2>) rc.getValues().iterator(), collector, DatatypeHelper.createDummyReporter());
//                }
//            } catch (IOException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
        }
        groupStarted = false;
    }

    private boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = groupFields[i];
            int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength() + a1.getFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength() + a2.getFieldStartOffset(t2Idx, fIdx);
            int l2 = a2.getFieldLength(t2Idx, fIdx);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws HyracksDataException {
//        closeCalled = true;
        if (groupStarted) {
//System.out.println("[ReduceWriter][close] calling reduce()");            
            reduce();
        }
        ClassLoader jobCL = helper.getJob().getConfiguration().getClassLoader();

        if (this.isNewAPI) {
            try {
                ((RecordWriter<K3, V3>) recordWriter).close((TaskAttemptContext) taskAttemptContext);
//                helper.getJob().getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());

//                FileOutputFormat fof = (FileOutputFormat) this.helper.getOutputFormat(ctx.getJobletContext()
//                        .getClassLoader());
//                Path dwf = fof.getDefaultWorkFile((TaskAttemptContext) taskAttemptContext, ""); //
//                Path op = fof.getOutputPath(new ContextFactory().createJobContext(helper.getConfiguration()));
//
//                FileSystem fs = dwf.getFileSystem(((TaskAttemptContext) taskAttemptContext).getConfiguration());
//                if (fs.exists(dwf.getParent())) {
//                    if (!fs.rename(dwf, op)) {
//                        throw new IOException("Could not rename " + dwf + " to " + op);
//                    }
//                }
                
//                Path outputPath = new Path(helper.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));
                String outputDir = helper.getConfiguration().get("pig.reduce.output.dirs");
                if(outputDir == null)
                    outputDir = helper.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
                
                String[] outputDirs = outputDir.split(",");
                
                for(String od: outputDirs){
                Path outputPath = new Path(od);
                
                FileSystem outputFS = outputPath
                        .getFileSystem(((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext)
                                .getConfiguration());
                Path workPath = new Path(outputPath, (org.apache.hadoop.mapred.FileOutputCommitter.TEMP_DIR_NAME
                        + Path.SEPARATOR + "0" + Path.SEPARATOR + org.apache.hadoop.mapred.FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR + ((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext)
                        .getTaskAttemptID().toString())).makeQualified(outputFS.getUri(), outputFS.getWorkingDirectory()); // out/_temporary/_attemp_..../
                  
                  ((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext).progress();
                  if (outputFS.exists(workPath)) {
                      moveTaskOutputs((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext,
                              outputFS, outputPath, workPath);
//                      if (!outputFileSystem.delete(workPath.getParent(), true)) {
//                          throw exception;
//                      }
                  }
                  
                }
                
//              FileOutputCommitter oc = (FileOutputCommitter)fof.getOutputCommitter((TaskAttemptContext)taskAttemptContext);
//              oc.commitTask((TaskAttemptContext)taskAttemptContext); //copying result to right directory and removing _temporary/task_attempt directory
//              Path ctp = oc.getCommittedTaskPath(taskAttemptContext);


                //        	System.out.println("workPath: " + oc.getWorkPath());
                //        	System.out.println("taPath: " + oc.getTaskAttemptPath(taskAttemptContext));
                //        	System.out.println("jaPath: " + oc.getJobAttemptPath(new ContextFactory().createJobContext(helper.getConfiguration())));
                //        	System.out.println("ctPath: " + oc.getCommittedTaskPath(taskAttemptContext));
                //        	System.out.println("outputPath: " + fof.getOutputPath(new ContextFactory().createJobContext(helper.getConfiguration())));
                //        	System.out.println("defaultWorkFile: " + fof.getDefaultWorkFile(taskAttemptContext, ""));

            } catch (Exception e) {
                throw new HyracksDataException(e);
            } finally {
                helper.getJob().getConfiguration().setClassLoader(jobCL);              
            }
        } else {
            try {
//                ((org.apache.hadoop.mapred.RecordWriter<K3, V3>) recordWriter).close(DatatypeHelper.createDummyReporter());
                ((RecordWriter<K3, V3>) recordWriter).close((TaskAttemptContext) taskAttemptContext);

//              Path outputPath = new Path(helper.getConfiguration().get("mapred.output.dir"));
                String outputDir = helper.getConfiguration().get("pig.reduce.output.dirs");
                if(outputDir == null)
                    outputDir = helper.getConfiguration().get("mapred.output.dir");
                
                String[] outputDirs = outputDir.split(",");
                
                for(String od: outputDirs){
                Path outputPath = new Path(od);
                
//                org.apache.hadoop.mapred.FileOutputFormat fof = (org.apache.hadoop.mapred.FileOutputFormat) helper.getOldOutputFormat();
//                Path outputPath = fof.getOutputPath(helper.getJobConf()); // out
                FileSystem outputFS = outputPath
                        .getFileSystem(((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext)
                                .getConfiguration());
                Path workPath = new Path(outputPath, (org.apache.hadoop.mapred.FileOutputCommitter.TEMP_DIR_NAME
                        + Path.SEPARATOR + "0" + Path.SEPARATOR + org.apache.hadoop.mapred.FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR + ((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext)
                        .getTaskAttemptID().toString())).makeQualified(outputFS.getUri(), outputFS.getWorkingDirectory()); // out/_temporary/_attemp_..../
                
                ((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext).progress();
                if (outputFS.exists(workPath)) {
                    moveTaskOutputs((org.apache.hadoop.mapreduce.TaskAttemptContext) taskAttemptContext,
                            outputFS, outputPath, workPath);
//                    if (!outputFileSystem.delete(workPath.getParent(), true)) {
//                        throw exception;
//                    }
                }
                }
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }
        System.gc();
    }

    private void moveTaskOutputs(org.apache.hadoop.mapreduce.TaskAttemptContext context, FileSystem fs,
            Path jobOutputDir, Path taskOutput) throws IOException {
        context.progress();
        if (fs.isFile(taskOutput)) {
//System.out.println("[ReduceWriter][moveTaskOutputs] taskOutput is File : " + taskOutput);
            Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, taskOutput);
//System.out.println("[ReduceWriter][moveTaskOutputs] finalOutputPath : " + finalOutputPath);            
            if (!fs.rename(taskOutput, finalOutputPath)) {
                if (!fs.delete(finalOutputPath, true)) {
                    throw new IOException("Failed to delete earlier output of task: " + taskAttemptContext);
                }
                if (!fs.rename(taskOutput, finalOutputPath)) {
                    throw new IOException("Failed to save output of task: " + taskAttemptContext);
                }
            }
        } else if (fs.getFileStatus(taskOutput).isDir()) {
//System.out.println("[ReduceWriter][moveTaskOutputs] taskOutput is Directory : " + taskOutput);            
            FileStatus[] paths = fs.listStatus(taskOutput);
//for(int i = 0; i < paths.length; i++)
//System.out.println("[ReduceWriter][moveTaskOutputs] paths : " + paths[i]);            
            Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, taskOutput);
//System.out.println("[ReduceWriter][moveTaskOutputs] finalOutputPath : " + finalOutputPath);            
            fs.mkdirs(finalOutputPath);
            if (paths != null) {
                for (FileStatus path : paths) {
                    moveTaskOutputs(context, fs, jobOutputDir, path.getPath());
                }
            }
        }
    }

    private Path getFinalPath(Path jobOutputDir, Path taskOutput, Path taskOutputPath) throws IOException {
        URI taskOutputUri = taskOutput.toUri();
        URI relativePath = taskOutputPath.toUri().relativize(taskOutputUri);
        if (taskOutputUri == relativePath) {
            throw new IOException("Can not get the relative path: base = " + taskOutputPath + " child = " + taskOutput);
        }
        if (relativePath.getPath().length() > 0) {
            return new Path(jobOutputDir, relativePath.getPath());
        } else {
            return jobOutputDir;
        }
    }

    //    public void cleanup() throws HyracksDataException { // removing the _temporary directory
    //      ClassLoader jobCL = helper.getJob().getConfiguration().getClassLoader();
    //        try {
    //        	helper.getJob().getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
    //        	FileOutputFormat fof = (FileOutputFormat)this.helper.getOutputFormat(ctx.getJobletContext().getClassLoader());
    ////        	FileOutputFormat fof = (FileOutputFormat)this.helper.getOutputFormat();
    //        	FileOutputCommitter oc = (FileOutputCommitter)fof.getOutputCommitter(taskAttemptContext);
    ////        	oc.abortTask(taskAttemptContext);
    //        	oc.cleanupJob(new ContextFactory().createJobContext(helper.getConfiguration()));
    //        } catch (Exception e) {
    //            throw new HyracksDataException(e);
    //        } finally {
    //        	helper.getJob().getConfiguration().setClassLoader(jobCL);
    //        }
    //    }

    @Override
    public void fail() throws HyracksDataException {
    }
}
