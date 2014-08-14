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
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.file.AbstractFileWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IRecordWriter;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;
import edu.uci.ics.hyracks.hdfs.ContextFactory;

public class MRWriteOperatorDescriptor<K2 extends Writable, V2 extends Writable, K3 extends Writable, V3 extends Writable> extends AbstractFileWriteOperatorDescriptor {
    private class HadoopFileWriter<K2, V2, K3, V3> implements IRecordWriter {
        Object recordWriter = null;
        
        Configuration conf;
        JobConf job;
        HadoopHelper helper;
        
//        Object taId;
//        FileOutputFormat fof;
        TaskAttemptID taId;
        TaskAttemptContext taskAttemptContext;
//        RecordDescriptor recordDescriptor = helper.getMapOutputRecordDescriptor(ctx.getJobletContext().getClassLoader());

        HadoopFileWriter(int index) throws Exception {
            this.conf = mConf.get();
            this.job = new JobConf(this.conf);
            this.helper = new HadoopHelper(mConf);
            initialize(index, this.conf);
        }

        private void initialize(int index, Configuration conf) throws Exception {       
            if (job.getUseNewMapper()){
                outputPath = new Path(helper.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));
            } else {
                outputPath = new Path(helper.getConfiguration().get("mapred.output.dir"));
            }
            
            this.taId = new TaskAttemptID(new SimpleDateFormat("YYYYMMddHHmm").format(new Date()), index, TaskType.MAP, index, 0);
            this.taskAttemptContext = new ContextFactory().createContext(helper.getConfiguration(), taId);
            recordWriter = helper.getOutputFormat().getRecordWriter((TaskAttemptContext)taskAttemptContext);
        }

        @Override
        public void write(Object[] record) throws Exception {
            if (recordWriter != null) {
//                if (job.getUseNewMapper()) { //////////////////////////
                
/// DEBUG PRINT ///                
//System.out.println("[" + Thread.currentThread().getId() + "][MRWriteOperatorDescriptor][write] keyClass: " + record[0].getClass() + ", valueClass: " + record[1].getClass());
//System.out.println("------------ key: " + record[0] + ", value: " + record[1] + " --------------------");
                
                    ((org.apache.hadoop.mapreduce.RecordWriter) recordWriter).write(record[0], record[1]);
//                } else {
                    
/// DEBUG PRINT ///                    
//System.out.println("------------ key: " + record[0] + ", value: " + record[1] + " --------------------");
//((org.apache.hadoop.mapred.RecordWriter) recordWriter).write(record[0], record[1]);
                    
//                }
            }
        }

        @Override
        public void close() {
            try {
                if (recordWriter != null) {
//                    if (job.getUseNewMapper()) { //////////////////////
                        ((org.apache.hadoop.mapreduce.RecordWriter) recordWriter).close(new ContextFactory().createContext(conf, new TaskAttemptID()));
//                    } else {
//                        ((org.apache.hadoop.mapred.RecordWriter) recordWriter).close(null);
//                    }
                    
                    if (outputPath != null) {                       
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
//                              if (!outputFileSystem.delete(workPath.getParent(), true)) {
//                                  throw exception;
//                              }
                          }
                            
                            
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        private void moveTaskOutputs(org.apache.hadoop.mapreduce.TaskAttemptContext context, FileSystem fs,
                Path jobOutputDir, Path taskOutput) throws IOException {
            context.progress();
            if (fs.isFile(taskOutput)) {
                Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, taskOutput);
                if (!fs.rename(taskOutput, finalOutputPath)) {
                    if (!fs.delete(finalOutputPath, true)) {
                        throw new IOException("Failed to delete earlier output of task: " + taskAttemptContext);
                    }
                    if (!fs.rename(taskOutput, finalOutputPath)) {
                        throw new IOException("Failed to save output of task: " + taskAttemptContext);
                    }
                }
            } else if (fs.getFileStatus(taskOutput).isDir()) {
                FileStatus[] paths = fs.listStatus(taskOutput);
                Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, taskOutput);
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
    }

    private static final long serialVersionUID = 1L;
    private MarshalledWritable<Configuration> mConf;
    private Path outputPath = null;
//    HadoopHelper helper;
//    private IHyracksTaskContext hCtx;

    @Override
    protected IRecordWriter createRecordWriter(FileSplit fileSplit, int index) throws Exception {
        return new HadoopFileWriter<K2, V2, K3, V3>(index);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
//        hCtx = ctx;
        return super.createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
    }

    private static FileSplit[] getOutputSplits(MarshalledWritable<Configuration> config, int noOfMappers)
            throws Exception {
        Configuration conf = config.get();
        JobContext jbct= new ContextFactory().createJobContext(conf);
        
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());
        Object outputFormat = ReflectionUtils.newInstance(
                jbct.getOutputFormatClass(), conf);

        FileSplit[] outputFileSplits = new FileSplit[noOfMappers];
        if (outputFormat instanceof NullOutputFormat) {
            for (int i = 0; i < noOfMappers; i++) {
                String outputPath = "/tmp/" + System.currentTimeMillis() + i;
                outputFileSplits[i] = new FileSplit("localhost", new FileReference(new File(outputPath)));
            }
            return outputFileSplits;
        } else {
            JobConf jobConf = new JobConf(conf);
            
            String path = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(jobConf).toString(); // new api returns relative path whereas old api returns absolute path
//            String path = FileOutputFormat.getgetOutputPath(jbct).toString(); 
            // hdfs://localhost:9000/grep or hdfs://localhost:9000/user/username/grep
//            if (!jobConf.getUseNewMapper()) { // if old api, make path as relative path
                Path workingDir = jobConf.getWorkingDirectory();// ex. hdfs://localhost:9000/user/username
                String defaultfs = conf.get("fs.defaultFS");    // ex. hdfs://localhost:9000
                path = path.replace(workingDir + "/", "");
                path = path.replace(defaultfs, "");
//            }

            for (int index = 0; index < noOfMappers; index++) {
                String suffix = new String("part-00000");
                suffix = new String(suffix.substring(0, suffix.length() - ("" + index).length()));
                suffix = suffix + index; // e.g. part-m-00004
                String outputPath = path + "/" + suffix;
                outputFileSplits[index] = new FileSplit("localhost", outputPath);
            }
            return outputFileSplits;
        }
    }

    private boolean checkIfCanWriteToHDFS(FileSplit[] fileSplits) throws Exception {
        try {
            FileSystem fileSystem = FileSystem.get(mConf.get());
            for (FileSplit fileSplit : fileSplits) {
                Path path = new Path(fileSplit.getLocalFile().getFile().getPath());
                if (fileSystem.exists(path)) {
                    throw new Exception(" Output path :  already exists : " + path);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }
        return true;
    }

    public MRWriteOperatorDescriptor(IOperatorDescriptorRegistry jobSpec, MarshalledWritable<Configuration> config,
            int numMapTasks) throws Exception {
        super(jobSpec, getOutputSplits(config, numMapTasks));
        this.mConf = config;
        checkIfCanWriteToHDFS(super.splits);
    }
}