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
package edu.uci.ics.hyracks.hadoop.compat.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
//import org.apache.hadoop.mapred.InputFormat;
//import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopMapperOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopReducerOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.HadoopWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopHashTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopNewPartitionerTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.WritableComparingBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.ClasspathBasedHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.IHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;

import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.HashPartitioningShuffleConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.IInputSplitProvider;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.MapperOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.ReducerOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.IInputSplitProviderFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.InputFileSplitProviderFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.MarshalledWritable;

public class NewHadoopAdapter {

    public static final String FS_DEFAULT_NAME = "fs.default.name";
    private Configuration conf;
    private Map<OperatorDescriptorId, Integer> operatorInstanceCount = new HashMap<OperatorDescriptorId, Integer>();
    public static final String HYRACKS_EX_SORT_FRAME_LIMIT = "HYRACKS_EX_SORT_FRAME_LIMIT";
    public static final int DEFAULT_EX_SORT_FRAME_LIMIT = 4096;
    public static final int DEFAULT_MAX_MAPPERS = 40;
    public static final int DEFAULT_MAX_REDUCERS = 40;
    public static final String MAX_MAPPERS_KEY = "maxMappers";
    public static final String MAX_REDUCERS_KEY = "maxReducers";
    public static final String EX_SORT_FRAME_LIMIT_KEY = "sortFrameLimit";

    private int maxMappers = DEFAULT_MAX_MAPPERS;
    private int maxReducers = DEFAULT_MAX_REDUCERS;
    private int exSortFrame = DEFAULT_EX_SORT_FRAME_LIMIT;

    class NewHadoopConstants {
        public static final String INPUT_FORMAT_CLASS_ATTR = "mapreduce.inputformat.class";
        public static final String MAP_CLASS_ATTR = "mapreduce.map.class";
        public static final String COMBINE_CLASS_ATTR = "mapreduce.combine.class";
        public static final String REDUCE_CLASS_ATTR = "mapreduce.reduce.class";
        public static final String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.outputformat.class";
        public static final String PARTITIONER_CLASS_ATTR = "mapreduce.partitioner.class";
    }

    public NewHadoopAdapter(String namenodeUrl) throws IOException {
        this.conf = new Configuration();
        conf.set(FS_DEFAULT_NAME, namenodeUrl);

        if (System.getenv(MAX_MAPPERS_KEY) != null) {
            maxMappers = Integer.parseInt(System.getenv(MAX_MAPPERS_KEY));
        }
        if (System.getenv(MAX_REDUCERS_KEY) != null) {
            maxReducers = Integer.parseInt(System.getenv(MAX_REDUCERS_KEY));
        }
        if (System.getenv(EX_SORT_FRAME_LIMIT_KEY) != null) {
            exSortFrame = Integer.parseInt(System
                    .getenv(EX_SORT_FRAME_LIMIT_KEY));
        }
    }

    private static RecordDescriptor getHadoopRecordDescriptor(
            String className1, String className2) {
        RecordDescriptor recordDescriptor = null;
        try {
            recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                    (Class<? extends Writable>) Class.forName(className1),
                    (Class<? extends Writable>) Class.forName(className2));
        } catch (ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        }
        return recordDescriptor;
    }

    private void configurePartitionCountConstraint(JobSpecification spec,
            IOperatorDescriptor operator, int instanceCount) {
        PartitionConstraintHelper.addPartitionCountConstraint(spec, operator,
                instanceCount);
        operatorInstanceCount.put(operator.getOperatorId(), instanceCount);
    }

    public MapperOperatorDescriptor getMapper(Job job,
            JobSpecification spec, IOperatorDescriptor previousOp)
            throws Exception {
//        boolean selfRead = (previousOp == null);
        InputFileSplitProviderFactory ispf = new InputFileSplitProviderFactory(job);

        MapperOperatorDescriptor mapOp = null;
        MarshalledWritable<Configuration> mConfig = new MarshalledWritable<Configuration>();
        mConfig.set(job.getConfiguration());

//        if (selfRead) {
            mapOp = new MapperOperatorDescriptor(spec, 1, mConfig, ispf);
            configurePartitionCountConstraint(spec, mapOp, ispf.getPartitionCnt());
//        }
/*******************************************************************************************************
        else {
            configurePartitionCountConstraint(spec, mapOp,
                    getInstanceCount(previousOp));
            mapOp = new MapperOperatorDescriptor(spec, 1, mConfig, inputSplitProviderFactory);
            spec.connect(new OneToOneConnectorDescriptor(spec), previousOp, 0,
                    mapOp, 0);
        }
*******************************************************************************************************/
        return mapOp;
    }

    public FileSystem getHDFSClient() {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(this.conf);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return fileSystem;
    }

    public Configuration getConf() {
        return conf;
    }

    public JobSpecification getJobSpecification(List<Configuration> confs)
            throws Exception {
        JobSpecification spec = null;
        if (confs.size() == 1) {
            spec = getJobSpecification(confs.get(0));
        } else {
            spec = getPipelinedSpec(confs);
        }
        return spec;
    }

    private int getInstanceCount(IOperatorDescriptor operator) {
        return operatorInstanceCount.get(operator.getOperatorId());
    }

    private int getNumReduceTasks(Job job) {
        int numReduceTasks = Math.min(maxReducers, job.getNumReduceTasks());
        return numReduceTasks;
    }

    private IOperatorDescriptor addReducer(
            IOperatorDescriptor previousOperator, Job job,
            JobSpecification spec) throws Exception {

        IOperatorDescriptor mrOutputOperator = previousOperator;

        if (job.getNumReduceTasks() != 0) {
            MarshalledWritable<Configuration> mConfig = new MarshalledWritable();
            mConfig.set(job.getConfiguration());
            IOperatorDescriptor reducer = new ReducerOperatorDescriptor(spec, 1, mConfig);

            int numReduceTasks = getNumReduceTasks(job);
            configurePartitionCountConstraint(spec, reducer, numReduceTasks);

            IConnectorDescriptor hpShffleConn = new HashPartitioningShuffleConnectorDescriptor(spec, mConfig);

            spec.connect(hpShffleConn, previousOperator, 0, reducer, 0);

            mrOutputOperator = reducer;
        }
        return mrOutputOperator;
    }
	
	public JobSpecification getPipelinedSpec(List<Configuration> confs)
	        throws Exception {
	    JobSpecification spec = new JobSpecification();
		Configuration firstMR = confs.get(0);
	    Job firstJob = new Job(firstMR);
	    IOperatorDescriptor mrOutputOp = configureMapReduce(null, spec, firstJob);
		for (int i = 1; i < confs.size(); i++) {
			Configuration currentConf = confs.get(i);
            Job currentJob = new Job(currentConf);
            mrOutputOp = configureMapReduce(mrOutputOp, spec,
                    currentJob);
        }
		spec.addRoot(mrOutputOp);
		System.out.println(spec);
	    return spec;
	}
	
	public JobSpecification getJobSpecification(Configuration conf) throws Exception {
	    JobSpecification spec = new JobSpecification();
	    Job job = new Job(conf);
	    IOperatorDescriptor mrOutput = configureMapReduce(null, spec, job);
	    spec.addRoot(mrOutput);
	    System.out.println(spec);
	    return spec;
	}
	
	private IOperatorDescriptor configureMapReduce(
	        IOperatorDescriptor previousOuputOp, JobSpecification spec,
	        Job job) throws Exception {
	    IOperatorDescriptor mapper = getMapper(job, spec, previousOuputOp);
	    IOperatorDescriptor reducer = addReducer(mapper, job, spec);
	    return reducer;
	}
	
	/*
	public static HashPartitioningShuffleConnectorDescriptor getHashPartitioningShuffleConnector(
	        IConnectorDescriptorRegistry spec, Job job) {
	
	    MarshalledWritable<Configuration> mConfig = new MarshalledWritable();
	    mConfig.set(job.getConfiguration());
	
	    Class mapOutputKeyClass = job.getMapOutputKeyClass();
	    Class mapOutputValueClass = job.getMapOutputValueClass();
	
	    HashPartitioningShuffleConnectorDescriptor connectorDescriptor = null;
	    ITuplePartitionComputerFactory factory = null;
	    job.getMapOutputKeyClass();
	    if (job.getPartitionerClass() != null
	            && !job.getPartitionerClass().getName().startsWith(
	                    "org.apache.hadoop")) {
	        Class<? extends Partitioner> partitioner = job
	                .getPartitionerClass();
	        factory = new HadoopNewPartitionerTuplePartitionComputerFactory(
	                partitioner, DatatypeHelper
	                        .createSerializerDeserializer(mapOutputKeyClass),
	                DatatypeHelper
	                        .createSerializerDeserializer(mapOutputValueClass));
	    } else {
	        RecordDescriptor recordDescriptor = DatatypeHelper
	                .createKeyValueRecordDescriptor(mapOutputKeyClass,
	                        mapOutputValueClass);
	        ISerializerDeserializer mapOutputKeySerializerDerserializer = DatatypeHelper
	                .createSerializerDeserializer(mapOutputKeyClass);
	        factory = new HadoopHashTuplePartitionComputerFactory(
	                mapOutputKeySerializerDerserializer);
	    }
	    connectorDescriptor = new MToNPartitioningConnectorDescriptor(spec,
	            factory);
	    return connectorDescriptor;
	}
	*/
}