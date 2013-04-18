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
package edu.uci.ics.hyracks.imru.dataflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Hashtable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.lib.TextKeyValueParserFactory;
import edu.uci.ics.hyracks.imru.api.TupleReader;
import edu.uci.ics.hyracks.imru.api.TupleWriter;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * This file can be removed.
 * 
 * @author Rui Wang
 */
public class Hdtest {
    static class HashGroupState extends AbstractStateObject {
        Hashtable<String, Integer> hash = new Hashtable<String, Integer>();

        public HashGroupState() {
        }

        HashGroupState(JobId jobId, Object id) {
            super(jobId, id);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
        }
    }

    public static FileSplit[] parseFileSplits(String fileSplits) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int t = s.indexOf(':');
            fSplits[i] = new FileSplit(s.substring(0, t), new FileReference(
                    new File(s.substring(t + 1))));
        }
        return fSplits;
    }

    public static void createPartitionConstraint(JobSpecification spec,
            IOperatorDescriptor op, FileSplit[] splits) {
        String[] parts = new String[splits.length];
        for (int i = 0; i < splits.length; i++)
            parts[i] = splits[i].getNodeName();
        PartitionConstraintHelper
                .addAbsoluteLocationConstraint(spec, op, parts);
    }

    public static JobSpecification createJob() throws Exception {
        JobSpecification spec = new JobSpecification();
        spec.setFrameSize(4096);

        String PATH_TO_HADOOP_CONF = "/home/wangrui/a/imru/hadoop-0.20.2/conf";
        String HDFS_INPUT_PATH = "/customer/customer.tbl,/customer_result/part-0";
        JobConf conf = new JobConf();
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
        conf.setInputFormat(TextInputFormat.class);
        RecordDescriptor recordDesc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });
        InputSplit[] splits = conf.getInputFormat().getSplits(conf, 1);
        HDFSReadOperatorDescriptor readOperator = new HDFSReadOperatorDescriptor(
                spec, recordDesc, conf, splits, new String[] { "NC0", "NC1" },
                new IKeyValueParserFactory<LongWritable, Text>() {
                    @Override
                    public IKeyValueParser<LongWritable, Text> createKeyValueParser(
                            final IHyracksTaskContext ctx) {
                        return new IKeyValueParser<LongWritable, Text>() {
                            TupleWriter tupleWriter;

                            @Override
                            public void open(IFrameWriter writer) {
                                tupleWriter = new TupleWriter(ctx, writer, 1);
                            }

                            @Override
                            public void parse(LongWritable key, Text value,
                                    IFrameWriter writer)
                                    throws HyracksDataException {
                                try {
                                    tupleWriter.write(value.getBytes(), 0,
                                            value.getLength());
                                    tupleWriter.finishField();
                                    tupleWriter.finishTuple();
                                } catch (IOException e) {
                                    throw new HyracksDataException(e);
                                }
                            }

                            @Override
                            public void close(IFrameWriter writer)
                                    throws HyracksDataException {
                                tupleWriter.close();
                            }
                        };
                    }

                });

        //        createPartitionConstraint(spec, readOperator, new String[] {"NC0"});
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
                readOperator, new String[] { "NC0", "NC1" });

        IOperatorDescriptor writer = new HDFSOD(spec, null, null, null);
        //        createPartitionConstraint(spec, writer, outSplits);

        spec.connect(new OneToOneConnectorDescriptor(spec), readOperator, 0,
                writer, 0);

        spec.addRoot(writer);
        return spec;
    }

    public static void main(String[] args) throws Exception {
        Rt.disableLogging();
        
        //start cluster controller
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 1099;
        ccConfig.clientNetPort = 3099;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 10;

        //start node controller
        ClusterControllerService cc = new ClusterControllerService(ccConfig);
        cc.start();

        for (int i = 0; i < 2; i++) {
            NCConfig config = new NCConfig();
            config.ccHost = "127.0.0.1";
            config.ccPort = 1099;
            config.clusterNetIPAddress = "127.0.0.1";
            config.dataIPAddress = "127.0.0.1";
            config.nodeId = "NC" + i;
            NodeControllerService nc = new NodeControllerService(config);
            nc.start();
        }

        //connect to hyracks
        IHyracksClientConnection hcc = new HyracksConnection("localhost", 3099);

        //update application
        hcc.createApplication("text", null);

        try {

            JobSpecification job = createJob();

            JobId jobId = hcc.startJob("text", job,
                    EnumSet.noneOf(JobFlag.class));
            hcc.waitForCompletion(jobId);

        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }
}