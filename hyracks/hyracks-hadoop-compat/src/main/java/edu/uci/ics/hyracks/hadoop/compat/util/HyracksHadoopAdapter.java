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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
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
//import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.MROneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.MapperOperatorDescriptor;
//import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.MapperOperatorDescriptorOldAPI;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.ReducerOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.IInputSplitProviderFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.InputSplitProviderFactory;
//import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.InputSplitProviderFactoryOldAPI;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.MarshalledWritable;
import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.MRWriteOperatorDescriptor;

//import edu.uci.ics.hyracks.dataflow.hadoop.mapreduce.ReducerOperatorDescriptorOldAPI;

public class HyracksHadoopAdapter {
    private static final Logger LOGGER = Logger.getLogger(HyracksHadoopAdapter.class.getName());


    public static final String FS_DEFAULT_NAME = "fs.default.name";
    private Configuration conf;
    private Map<OperatorDescriptorId, Integer> operatorInstanceCount = new HashMap<OperatorDescriptorId, Integer>();
    public static final String HYRACKS_EX_SORT_FRAME_LIMIT = "HYRACKS_EX_SORT_FRAME_LIMIT";
    public static final int DEFAULT_EX_SORT_FRAME_LIMIT = 4096;
    public static final int DEFAULT_MAX_MAPPERS = 40;
    public static final int DEFAULT_MAX_REDUCERS = 40;
    public static final String EX_SORT_FRAME_LIMIT_KEY = "sortFrameLimit";

    private int maxMappers;
    private int maxReducers;
    //    private int exSortFrame = DEFAULT_EX_SORT_FRAME_LIMIT;

    private boolean isNewAPI = true;
    private boolean useReducer = false;

    public HyracksHadoopAdapter(JobConf jobConf) throws IOException {
        this.conf = new Configuration(jobConf);
        this.isNewAPI = jobConf.getUseNewMapper();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("isNewAPI: " + this.isNewAPI);
        }
//        this.useReducer = !((jobConf.getNumReduceTasks() == 0) || ((this.isNewAPI == true ? jobConf
//                .get("mapreduce.reduce.class") : jobConf.get("mapred.reducer.class")) == null));
        this.useReducer = jobConf.getNumReduceTasks() == 0 ? false : true;
        maxMappers = DEFAULT_MAX_MAPPERS;
        maxReducers = DEFAULT_MAX_REDUCERS;
//        maxMappers = jobConf.getInt("mapred.tasktracker.map.tasks.maximum", 2);
//        maxReducers = jobConf.getInt("mapred.tasktracker.reduce.tasks.maximum", 2);
        
        //        if (System.getenv(EX_SORT_FRAME_LIMIT_KEY) != null) {
        //            exSortFrame = Integer.parseInt(System
        //                    .getenv(EX_SORT_FRAME_LIMIT_KEY));
        //        }
    }

    private void configurePartitionCountConstraint(JobSpecification spec, IOperatorDescriptor operator,
            int instanceCount) {
        PartitionConstraintHelper.addPartitionCountConstraint(spec, operator, instanceCount);
        operatorInstanceCount.put(operator.getOperatorId(), instanceCount);
    }

    public MapperOperatorDescriptor getMapper(JobConf jobConf, JobSpecification spec) throws Exception {
        InputSplitProviderFactory ispf = new InputSplitProviderFactory(this.conf);

        MapperOperatorDescriptor mapOp = null;
        MarshalledWritable<Configuration> mConfig = new MarshalledWritable<Configuration>();
        mConfig.set(jobConf);

        mapOp = new MapperOperatorDescriptor(spec, 1, mConfig, ispf);
        int partitionCount = ispf.getPartitionCnt();
        if (partitionCount == 0)
            partitionCount = jobConf.getNumMapTasks();
        configurePartitionCountConstraint(spec, mapOp, partitionCount);

        return mapOp;
    }

    public MapperOperatorDescriptor getOldMapper(JobConf jobConf, JobSpecification spec) throws Exception {
        InputSplitProviderFactory ispf = new InputSplitProviderFactory(jobConf);

        MapperOperatorDescriptor mapOp = null;
        MarshalledWritable<Configuration> mConfig = new MarshalledWritable<Configuration>();
        mConfig.set(jobConf);

        mapOp = new MapperOperatorDescriptor(spec, 1, mConfig, ispf);
        int partitionCount = ispf.getPartitionCnt();
        if (partitionCount == 0)
        	partitionCount = jobConf.getNumMapTasks();
        configurePartitionCountConstraint(spec, mapOp, partitionCount);

        return mapOp;
    }

    public Configuration getConf() {
        return conf;
    }

    private int getNumReduceTasks(JobConf jobConf) {
        int numReduceTasks = Math.min(maxReducers, jobConf.getNumReduceTasks());
        return numReduceTasks;
    }

    public int getMaxMappers() {
        return maxMappers;
    }

    public void setMaxMappers(int maxMappers) {
        this.maxMappers = maxMappers;
    }

    public int getMaxReducers() {
        return maxReducers;
    }

    public void setMaxReducers(int maxReducers) {
        this.maxReducers = maxReducers;
    }

    private IOperatorDescriptor addReducer(IOperatorDescriptor previousOperator, JobConf jobConf, JobSpecification spec)
            throws Exception {

        IOperatorDescriptor mrOutputOperator = previousOperator;

        if (this.useReducer) {
            MarshalledWritable<Configuration> mConfig = new MarshalledWritable();
            mConfig.set(jobConf);
            IOperatorDescriptor reducer = new ReducerOperatorDescriptor(spec, 1, mConfig);

            int instanceCountPreviousOperator = operatorInstanceCount.get(previousOperator.getOperatorId());
            int numReduceTasks = getNumReduceTasks(jobConf) != 0 ? getNumReduceTasks(jobConf)
                    : instanceCountPreviousOperator;
            
            configurePartitionCountConstraint(spec, reducer, numReduceTasks);

            IConnectorDescriptor hpShffleConn = new HashPartitioningShuffleConnectorDescriptor(spec, mConfig);

            spec.connect(hpShffleConn, previousOperator, 0, reducer, 0);

            mrOutputOperator = reducer;
        }
        return mrOutputOperator;
    }

    private IOperatorDescriptor addOldReducer(IOperatorDescriptor previousOperator, JobConf jobConf,
            JobSpecification spec) throws Exception {

        IOperatorDescriptor mrOutputOperator = previousOperator;

        if (this.useReducer) {
            MarshalledWritable<Configuration> mConfig = new MarshalledWritable();
            mConfig.set(jobConf);
            IOperatorDescriptor reducer = new ReducerOperatorDescriptor(spec, 1, mConfig);
            
            int instanceCountPreviousOperator = operatorInstanceCount.get(previousOperator.getOperatorId());            
            int numReduceTasks = getNumReduceTasks(jobConf) != 0 ? getNumReduceTasks(jobConf) : instanceCountPreviousOperator;
            configurePartitionCountConstraint(spec, reducer, numReduceTasks);

            IConnectorDescriptor hpShffleConn = new HashPartitioningShuffleConnectorDescriptor(spec, mConfig);

            spec.connect(hpShffleConn, previousOperator, 0, reducer, 0);

            mrOutputOperator = reducer;
        }
        return mrOutputOperator;
    }

    private IOperatorDescriptor addNoReducer(IOperatorDescriptor previousOperator, JobConf jobConf,
            JobSpecification spec) throws Exception {
        int instanceCountPreviousOperator = operatorInstanceCount.get(previousOperator.getOperatorId());
        MarshalledWritable<Configuration> mConfig = new MarshalledWritable<Configuration>();
        mConfig.set(jobConf);

        MRWriteOperatorDescriptor writer = null;
        writer = new MRWriteOperatorDescriptor(spec, mConfig, instanceCountPreviousOperator); //numOutputters);
        configurePartitionCountConstraint(spec, writer, instanceCountPreviousOperator); //numOutputters);
        spec.connect(new OneToOneConnectorDescriptor(spec), previousOperator, 0, writer, 0);
        return writer;
        
//        IOperatorDescriptor reducer = new ReducerOperatorDescriptor(spec, 1, mConfig);
//        configurePartitionCountConstraint(spec, reducer, instanceCountPreviousOperator); //numOutputters);
//        spec.connect(new OneToOneConnectorDescriptor(spec), previousOperator, 0, reducer, 0);
//        return reducer;
    }
    
    public JobSpecification getJobSpecification(JobConf jobConf) throws Exception {
        JobSpecification spec = new JobSpecification();
        spec.setProperty("isNewAPI", this.isNewAPI);
        IOperatorDescriptor mrOutput = configureMapReduce(spec, jobConf);
        spec.addRoot(mrOutput);
System.out.println(spec);
        return spec;
    }

    private IOperatorDescriptor configureMapReduce(JobSpecification spec, JobConf jobConf) throws Exception {
        IOperatorDescriptor mapper = null;
        IOperatorDescriptor reducer = null;

        if (jobConf.getUseNewMapper()) {
            mapper = getMapper(jobConf, spec);
            if (this.useReducer)
                reducer = addReducer(mapper, jobConf, spec);
            else
                reducer = addNoReducer(mapper, jobConf, spec);
        } else {
            mapper = getOldMapper(jobConf, spec);
            if (this.useReducer)
                reducer = addOldReducer(mapper, jobConf, spec);
            else
                reducer = addNoReducer(mapper, jobConf, spec);
        }

        return reducer;
    }
}