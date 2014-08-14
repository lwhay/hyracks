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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;    // org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;         //org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;   // org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
                                                    // org.apache.hadoop.mapred.FileOutputCommitter;




import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.hdfs.ContextFactory;

public class ReducerOperatorDescriptor<K2 extends Writable, V2 extends Writable, K3 extends Writable, V3 extends Writable>
        extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final int jobId;
    private boolean isNewAPI = true;
    private MarshalledWritable<Configuration> mConfig;

    public ReducerOperatorDescriptor(IOperatorDescriptorRegistry spec, int jobId, MarshalledWritable<Configuration> mConfig) {
        super(spec, 1, 0);
        this.jobId = jobId;
        this.mConfig = mConfig;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final HadoopHelper helper = new HadoopHelper(mConfig);
        JobConf jb = (JobConf)helper.getConfiguration();
        this.isNewAPI = jb.getUseNewReducer();
        
        final Object reducer;
        if(this.isNewAPI)
            reducer = helper.getReducer(ctx.getJobletContext().getClassLoader());
        else
            reducer = helper.getOldReducer(ctx.getJobletContext().getClassLoader());
//        final Reducer<K2, V2, K3, V3> reducer = helper.getReducer(ctx.getJobletContext().getClassLoader()); //////////////////////////
        
// [FYI] Here, 
// helper.getConfiguration().getClass().getClassLoader() = AppClassLoader
// Thread.currentThread().getContextClassLoader()= AppClassLoader
// this.getClass().getClassLoader()= AppClassLoader        
        
        final RecordDescriptor recordDescriptor = helper.getMapOutputRecordDescriptor(ctx.getJobletContext().getClassLoader());
        final int[] groupFields = helper.getSortFields();
        IBinaryComparatorFactory[] groupingComparators = helper.getGroupingComparatorFactories(ctx.getJobletContext().getClassLoader());

       
//        final Object taId;
//        if(this.isNewAPI)
//            taId = new TaskAttemptID(new SimpleDateFormat("YYYYMMddHHmm").format(new Date()), jobId, TaskType.REDUCE, partition, 0);
//        else 
//            taId = new org.apache.hadoop.mapred.TaskAttemptID(new SimpleDateFormat("YYYYMMddHHmm").format(new Date()), jobId, false, partition, 0);
        final TaskAttemptID taId = new TaskAttemptID(new SimpleDateFormat("YYYYMMddHHmm").format(new Date()), jobId, TaskType.REDUCE, partition, 0);
        
        
//        final Object taskAttemptContext;
//        if(this.isNewAPI)
//            taskAttemptContext = new ContextFactory().createContext(helper.getConfiguration(), (TaskAttemptID)taId);//helper.createTaskAttemptContext((TaskAttemptID)taId);
//        else
//            taskAttemptContext = new ContextFactory().createContext(jb, (org.apache.hadoop.mapred.TaskAttemptID)taId);//helper.createTaskAttemptContext((org.apache.hadoop.mapred.TaskAttemptID)taId);  
        final TaskAttemptContext taskAttemptContext = new ContextFactory().createContext(helper.getConfiguration(), taId);//helper.createTaskAttemptContext((org.apache.hadoop.mapred.TaskAttemptID)taId);
        
        final Object recordWriter;
//        final RecordWriter recordWriter;
        
        ClassLoader jobCL = helper.getJob().getConfiguration().getClassLoader();
        try {
        	helper.getJob().getConfiguration().setClassLoader(ctx.getJobletContext().getClassLoader());
        	
        	if(this.isNewAPI){
                recordWriter = helper.getOutputFormat().getRecordWriter((TaskAttemptContext)taskAttemptContext);        	    
        	}
        	else {
        	    helper.getConfiguration().set("mapred.task.id", taId.toString());
//                FileSystem fs = FileSystem.get(helper.getJobConf());
//                
//                Path outputPath = new Path(helper.getJobConf().get("mapred.output.dir"));
//                Path tempDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
//                FileSystem fileSys = tempDir.getFileSystem(helper.getJobConf());
//                if (!fileSys.mkdirs(tempDir)) {
//                    throw new IOException("Mkdirs failed to create " + tempDir.toString());
//                }
//                
//                String suffix = new String("part-00000");
//                suffix = new String(suffix.substring(0, suffix.length() - ("" + partition).length()));
//                suffix = suffix + partition;
//                recordWriter = helper.getOldOutputFormat().getRecordWriter(fs, helper.getJobConf(), suffix, DatatypeHelper.createDummyReporter());
        	    
        	    // just for hive jobs //
        	    String outputdir = helper.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        	    String hiveoutput = helper.getConfiguration().get("hive.metastore.warehouse.dir");
        	    if( outputdir == null && hiveoutput != null )
        	        helper.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", hiveoutput);
        	    ////////////////////////
        	    
                recordWriter = helper.getOutputFormat().getRecordWriter((TaskAttemptContext)taskAttemptContext);
           	}
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
        	helper.getJob().getConfiguration().setClassLoader(jobCL);
        }

        final ReduceWriter<K2, V2, K3, V3> rw = new ReduceWriter<K2, V2, K3, V3>(ctx, helper, groupFields,
                groupingComparators, recordDescriptor, reducer, recordWriter, taId, taskAttemptContext);

        return new AbstractUnaryInputSinkOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                rw.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                rw.nextFrame(buffer);
            }

            @Override
            public void close() throws HyracksDataException {
                rw.close();
//                rw.cleanup();
            }

            @Override
            public void fail() throws HyracksDataException {
            }
        };
    }
}