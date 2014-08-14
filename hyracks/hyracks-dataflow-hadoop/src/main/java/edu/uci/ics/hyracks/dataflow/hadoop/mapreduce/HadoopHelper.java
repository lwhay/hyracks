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

import java.io.IOException;
//import java.util.Iterator;
import java.util.List;
//import java.util.Vector;
//import java.lang.instrument.*;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat; // org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;  // org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.Job;         // org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;  // org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;      // org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;// org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;     // org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;  // org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;       // org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;
                                                        // org.apache.hadoop.mapred.Reporter;
                                                        // org.apache.hadoop.mapred.Counters.Counter;








import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopNewPartitionerTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopPartitionerTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.WritableComparingBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.hdfs.ContextFactory;

public class HadoopHelper {
    public static final int KEY_FIELD_INDEX = 0;
    public static final int VALUE_FIELD_INDEX = 1;
    public static final int BLOCKID_FIELD_INDEX = 2;
    private static final int[] KEY_SORT_FIELDS = new int[] { 0 };

    private MarshalledWritable<Configuration> mConfig;
    private Configuration config;
    private Job job;
//    private Reporter reporter;
//    private JobConf jobConf;

    public HadoopHelper(MarshalledWritable<Configuration> mConfig) throws HyracksDataException { // ctxCL: URLClassLoader, getClass(): AppClassLoader, config: AppClassLoader
        this.mConfig = mConfig;
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();        /////////
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader()); /////////
            config = mConfig.get();
            config.setClassLoader(getClass().getClassLoader()); /////////////////////////
            job = Job.getInstance(config);
//            jobConf = (JobConf)job.getConfiguration();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL); /////////////////////
        }
    }
    
    public RecordDescriptor getMapOutputRecordDescriptor() throws HyracksDataException { // used in MapperOperatorDescriptor's Constructor
        try {
            job.getConfiguration().setClassLoader(Thread.currentThread().getContextClassLoader());
//System.out.println("[HadoopHelper][getMapOutputRecordDescriptor(NOCL)] keyClass: " + job.getMapOutputKeyClass() + ", valueClass: " + job.getMapOutputValueClass());
            return new RecordDescriptor(
                    new ISerializerDeserializer[] {
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputKeyClass()),
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputValueClass()), IntegerSerializerDeserializer.INSTANCE });

        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
    
    public <K2> Class<K2> getMapOutputKeyClass(ClassLoader cl) throws HyracksDataException {
        ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
            job.getConfiguration().setClassLoader(cl);
            return (Class<K2>) job.getMapOutputKeyClass();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            job.getConfiguration().setClassLoader(jobCL);
        }
    }
    
    public <V2> Class<V2> getMapOutputValueClass(ClassLoader cl) throws HyracksDataException {
        ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
            job.getConfiguration().setClassLoader(cl);
            return (Class<V2>) job.getMapOutputValueClass();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            job.getConfiguration().setClassLoader(jobCL);
        }
    }

    public RecordDescriptor getMapOutputRecordDescriptor(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
        	job.getConfiguration().setClassLoader(cl);
//System.out.println("[HadoopHelper][getMapOutputRecordDescriptor(CL)] keyClass: " + job.getMapOutputKeyClass() + ", valueClass: " + job.getMapOutputValueClass());
        	
        	return new RecordDescriptor(
                    new ISerializerDeserializer[] {
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputKeyClass()),
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputValueClass()), IntegerSerializerDeserializer.INSTANCE });
        } catch (Exception e) {
          throw new HyracksDataException(e);
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
		}
    }

    public RecordDescriptor getMapOutputRecordDescriptorWithoutExtraFields(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
        	job.getConfiguration().setClassLoader(cl);
        	return new RecordDescriptor(
                    new ISerializerDeserializer[] {
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputKeyClass()),
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputValueClass()) });
        } catch (Exception e) {
            throw new HyracksDataException(e);
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
		}
    }

//    public TaskAttemptContext createTaskAttemptContext(TaskAttemptID taId) throws HyracksDataException {
//        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
//        try {
//            Thread.currentThread().setContextClassLoader(config.getClassLoader());
//            return new ContextFactory().createContext(config, taId);
//        } catch (HyracksDataException e) {
//            throw new HyracksDataException(e);
//        } finally {
//            Thread.currentThread().setContextClassLoader(ctxCL);
//        }
//    }
//    
//    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapred.TaskAttemptID taId) throws HyracksDataException {
//        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
//        try {
//            Thread.currentThread().setContextClassLoader(config.getClassLoader());
//            return new ContextFactory().createContext(jobConf, taId); 
//        } catch (HyracksDataException e) {
//            throw new HyracksDataException(e);
//        } finally {
//            Thread.currentThread().setContextClassLoader(ctxCL);
//        }
//    }
//
//    public JobContext createJobContext() {
//        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
//        try {
//            Thread.currentThread().setContextClassLoader(config.getClassLoader());
//            return new ContextFactory().createJobContext(config);
//        } finally {
//            Thread.currentThread().setContextClassLoader(ctxCL);
//        }
//    }

    public <K1, V1, K2, V2> Mapper<K1, V1, K2, V2> getMapper(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
        	job.getConfiguration().setClassLoader(cl);
	        return (Mapper<K1, V1, K2, V2>) ReflectionUtils.newInstance(job.getMapperClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
		}
    }
    
    public <K1, V1, K2, V2> org.apache.hadoop.mapred.Mapper<K1, V1, K2, V2> getOldMapper(ClassLoader cl) throws HyracksDataException {
        ClassLoader jobCL = config.getClassLoader();
        try {
            config.setClassLoader(cl);
            return (org.apache.hadoop.mapred.Mapper<K1, V1, K2, V2>) HadoopTools.newInstance( new JobConf(config).getMapperClass() ); // ReflectionUtils.newInstance(jobConf.getMapperClass(), config);
        } catch (InstantiationException e) {
            throw new HyracksDataException(e);
        } catch (IllegalAccessException e) {
            throw new HyracksDataException(e);
        } catch (NoSuchMethodException e) {
            throw new HyracksDataException(e);
        } catch (SecurityException e) {
            throw new HyracksDataException(e);
        } catch (IllegalArgumentException e) {
            throw new HyracksDataException(e);
        } catch (InvocationTargetException e) {
            throw new HyracksDataException(e);
        } finally {
            config.setClassLoader(jobCL);
        }
    }
    
    public <K2, V2, K3, V3> Reducer<K2, V2, K3, V3> getReducer(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
    	try {
    		job.getConfiguration().setClassLoader(cl);
    		return (Reducer<K2, V2, K3, V3>) ReflectionUtils.newInstance(job.getReducerClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
    	}
    }
    
    public <K2, V2, K3, V3> org.apache.hadoop.mapred.Reducer<K2, V2, K3, V3> getOldReducer(ClassLoader cl) throws HyracksDataException {
        ClassLoader jobCL = config.getClassLoader();
        try {
            config.setClassLoader(cl);
            return (org.apache.hadoop.mapred.Reducer<K2, V2, K3, V3>) HadoopTools.newInstance( new JobConf(config).getReducerClass() ); // ReflectionUtils.newInstance(jobConf.getReducerClass(), config); //HadoopTools.newInstance(jobConf.getReducerClass());
        } catch (InstantiationException e) {
            throw new HyracksDataException(e);
        } catch (IllegalAccessException e) {
            throw new HyracksDataException(e);
        } catch (NoSuchMethodException e) {
            throw new HyracksDataException(e);
        } catch (SecurityException e) {
            throw new HyracksDataException(e);
        } catch (IllegalArgumentException e) {
            throw new HyracksDataException(e);
        } catch (InvocationTargetException e) {
            throw new HyracksDataException(e);
        } finally {
            config.setClassLoader(jobCL);
        }
    }

    public <K2, V2> Reducer<K2, V2, K2, V2> getCombiner(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
			job.getConfiguration().setClassLoader(cl);
			return (Reducer<K2, V2, K2, V2>) ReflectionUtils.newInstance(job.getCombinerClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
    	}
    }
    
    public <K2, V2> org.apache.hadoop.mapred.Reducer<K2, V2, K2, V2> getOldCombiner(ClassLoader cl) throws HyracksDataException {
        ClassLoader jobCL = config.getClassLoader();
        try {
            config.setClassLoader(cl);
            try {
                return (org.apache.hadoop.mapred.Reducer<K2, V2, K2, V2>) HadoopTools.newInstance( new JobConf(config).getCombinerClass() );
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException
                    | IllegalArgumentException | InvocationTargetException e) {
                throw new HyracksDataException(e);
            }

        } finally {
            config.setClassLoader(jobCL);
        }
    }

    @SuppressWarnings("unchecked")
    public <K, V> InputFormat<K, V> getInputFormat(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
    		job.getConfiguration().setClassLoader(cl);
    		return (InputFormat<K, V>) ReflectionUtils.newInstance(job.getInputFormatClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        } finally {
    		job.getConfiguration().setClassLoader(jobCL);
    	}
    }
    
//    public <K, V> InputFormat<K, V> getInputFormat() throws HyracksDataException { // used in MapperOperatorDescriptor Constructor
//            try {
//                return (InputFormat<K, V>) ReflectionUtils.newInstance(job.getInputFormatClass(), config);
//            } catch (ClassNotFoundException e) {
//                throw new HyracksDataException(e);
//            }
//    }
    
    public <K, V> org.apache.hadoop.mapred.InputFormat<K, V> getOldInputFormat(ClassLoader cl) throws HyracksDataException {
        ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
            job.getConfiguration().setClassLoader(cl);
            return (org.apache.hadoop.mapred.InputFormat<K, V>) new JobConf(config).getInputFormat();
        } finally {
            job.getConfiguration().setClassLoader(jobCL);
        }
    }    

    public <K, V> List<InputSplit> getInputSplits() throws HyracksDataException {
    	if ( (config.get("mapred.input.dir") != null) || 
    			((config.get("mapred.input.dir") == null) && (config.get("mapreduce.inputformat.class") != null)) ) {
    			// normal case || no input directory but internal input generator
            InputFormat<K, V> fmt = getInputFormat(Thread.currentThread().getContextClassLoader());
            JobContext jCtx = new ContextFactory().createJobContext(config);
            try {
            	List<InputSplit> lis = fmt.getSplits(jCtx);
            	return lis;
            } catch (IOException e) {
                throw new HyracksDataException(e);
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
    	}
    	
    	return null;
    }
    
    public <K, V> org.apache.hadoop.mapred.InputSplit[] getOldInputSplits() throws HyracksDataException {
        if ( (config.get("mapred.input.dir") != null) || 
                ((config.get("mapred.input.dir") == null) && (config.get("mapred.input.format.class") != null)) ) {
                // normal case || no input directory but internal input generator
            org.apache.hadoop.mapred.InputFormat<K, V> fmt = getOldInputFormat(Thread.currentThread().getContextClassLoader());
            try {
                JobConf jc = new JobConf(config);
                org.apache.hadoop.mapred.InputSplit[] isa = fmt.getSplits(jc, jc.getNumMapTasks());
                return isa;
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        
        return null;
    }

    public IBinaryComparatorFactory[] getSortComparatorFactories(ClassLoader cl) {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
			job.getConfiguration().setClassLoader(cl);
			RawComparator<?> cmptr = job.getSortComparator();
			Class clazz = cmptr.getClass();
   			WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(clazz, cmptr);
   			return new IBinaryComparatorFactory[] { comparatorFactory };
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
    	}	
    }
    
    public IBinaryComparatorFactory[] getGroupingComparatorFactories() {
        RawComparator<?> cmptr = job.getGroupingComparator();
        Class clazz = cmptr.getClass();
        WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(clazz, cmptr);

        return new IBinaryComparatorFactory[] { comparatorFactory };
    }
    
    public IBinaryComparatorFactory[] getGroupingComparatorFactories(ClassLoader cl) {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
            job.getConfiguration().setClassLoader(cl);
            RawComparator<?> cmptr = job.getGroupingComparator();
            Class clazz = cmptr.getClass();
			WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(clazz, cmptr);

	        return new IBinaryComparatorFactory[] { comparatorFactory };
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
    	}	
    }

    public RawComparator<?> getRawGroupingComparator(ClassLoader cl) {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
			job.getConfiguration().setClassLoader(cl);
			return job.getGroupingComparator();
		} finally {
    		job.getConfiguration().setClassLoader(jobCL);
    	}
    }
    
    public RawComparator<?> getRawGroupingComparator() {
            return job.getGroupingComparator();
    }

    public int getSortFrameLimit(IHyracksCommonContext ctx) {
        int sortMemory = job.getConfiguration().getInt("mapreduce.task.io.sort.mb", 100); // 1.x: io.sort.mb, 2.x: mapreduce.task.io.sort.mb
        
        return (int) (((long) sortMemory * 1024 * 1024) / ctx.getFrameSize());
    }

    public Job getJob() {
        return job;
    }

    public MarshalledWritable<Configuration> getMarshalledConfiguration() {
        return mConfig;
    }

    public Configuration getConfiguration() {
        return config;
    }

    public ITuplePartitionComputerFactory getTuplePartitionComputer(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
        	job.getConfiguration().setClassLoader(cl);
            return new HadoopNewPartitionerTuplePartitionComputerFactory<Writable, Writable>(
                    (Class<? extends Partitioner<Writable, Writable>>) job.getPartitionerClass(),
                    (ISerializerDeserializer<Writable>) DatatypeHelper
                            .createSerializerDeserializer((Class<? extends Writable>) job.getMapOutputKeyClass()),
                    (ISerializerDeserializer<Writable>) DatatypeHelper
                            .createSerializerDeserializer((Class<? extends Writable>) job.getMapOutputValueClass()), job.getConfiguration());
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        } finally {
    		job.getConfiguration().setClassLoader(jobCL);
    	}
    }
    
    public ITuplePartitionComputerFactory getOldTuplePartitionComputer(ClassLoader cl) throws HyracksDataException {
        JobConf jc = new JobConf(config);
        ClassLoader jobCL = jc.getClassLoader();
        try {
            jc.setClassLoader(cl); 
            return new HadoopPartitionerTuplePartitionComputerFactory<Writable, Writable>(
                    (Class<? extends org.apache.hadoop.mapred.Partitioner<Writable, Writable>>) jc.getPartitionerClass(),
                    (ISerializerDeserializer<Writable>) DatatypeHelper
                            .createSerializerDeserializer((Class<? extends Writable>) jc.getMapOutputKeyClass()),
                    (ISerializerDeserializer<Writable>) DatatypeHelper
                            .createSerializerDeserializer((Class<? extends Writable>) jc.getMapOutputValueClass()), jc);
//        } catch (ClassNotFoundException e) {
//            throw new HyracksDataException(e);
        } finally {
            jc.setClassLoader(jobCL);
        }
    }

    public int[] getSortFields() {
        return KEY_SORT_FIELDS;
    }

    public <K> ISerializerDeserializer<K> getMapOutputKeySerializerDeserializer() {
        return (ISerializerDeserializer<K>) DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                .getMapOutputKeyClass());
    }

    public <V> ISerializerDeserializer<V> getMapOutputValueSerializerDeserializer() {
        return (ISerializerDeserializer<V>) DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                .getMapOutputValueClass());
    }

    public FileSystem getFilesystem() throws HyracksDataException {
        try {
            return FileSystem.get(config);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public <K, V> OutputFormat<K, V> getOutputFormat(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
        	job.getConfiguration().setClassLoader(cl);
            return (OutputFormat<K, V>) ReflectionUtils.newInstance(job.getOutputFormatClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        } finally {
    		job.getConfiguration().setClassLoader(jobCL);
		}
    }
    
    public <K, V> OutputFormat<K, V> getOutputFormat() throws HyracksDataException {
        try {
            return (OutputFormat<K, V>) ReflectionUtils.newInstance(job.getOutputFormatClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
		}
    }
    
    public <K, V> org.apache.hadoop.mapred.OutputFormat<K, V> getOldOutputFormat() throws HyracksDataException {
          return (org.apache.hadoop.mapred.OutputFormat<K, V>) new JobConf(config).getOutputFormat();
    }
    
    public <K, V> org.apache.hadoop.mapred.OutputFormat<K, V> getOldOutputFormat(ClassLoader cl) throws HyracksDataException {
        ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
            job.getConfiguration().setClassLoader(cl);
            return (org.apache.hadoop.mapred.OutputFormat<K, V>) new JobConf(config).getOutputFormat();
//        } catch (ClassNotFoundException e) {
//            throw new HyracksDataException(e);
        } finally {
            job.getConfiguration().setClassLoader(jobCL);
        }
    }

    public boolean hasCombiner(ClassLoader cl) throws HyracksDataException {
    	ClassLoader jobCL = job.getConfiguration().getClassLoader();
        try {
        	job.getConfiguration().setClassLoader(cl);
            return (job.getCombinerClass() != null) || (config.get("mapred.combiner.class") != null);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        } finally {
    		job.getConfiguration().setClassLoader(jobCL);
		}
    }
    
    public boolean getUseNewMapper(){
        return config.getBoolean("mapred.mapper.new-api",  false);
    }
    
    public boolean getUseNewReducer() {
        return config.getBoolean("mapred.reducer.new-api", false);
    }
    
    public JobConf getJobConf() {
        return new JobConf(config);
    } 
    
}