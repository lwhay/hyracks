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
package edu.uci.ics.hyracks.example.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Hashtable;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * This is a all-in-one word count example to test out hyracks data flow
 * Input file 1: 0a 1b 1c
 * Input file 2: 0b 1c 1c
 * There are two output files. All words starts 0 should be
 * written to output file 0. All words starts 1 should be
 * written to output file 1.
 * For each output file, list words and frequences.
 * Correct output:
 * File 1:
 * 1 0b
 * 1 0a
 * File 2:
 * 3 1c
 * 1 1b
 * This program print out hyracks internal process flow.
 * 
 * @author Rui Wang
 *         Reference: hyracks/hyracks-examples/text-example/textclient/src/main/java/edu/uci/ics/hyracks/examples/text/client
 */
public class WordCountAllInOneExample {
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

    public static JobSpecification createJob(final FileSplit[] inSplits,
            final FileSplit[] outSplits) {
        JobSpecification spec = new JobSpecification();
        spec.setFrameSize(256);

        IOperatorDescriptor reader = new AbstractSingleActivityOperatorDescriptor(
                spec, 0, 1) {
            {
                recordDescriptors[0] = new RecordDescriptor(
                        new ISerializerDeserializer[1]);
            }

            @Override
            public IOperatorNodePushable createPushRuntime(
                    final IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider,
                    final int partition, int nPartitions) {
                final FileSplit split = inSplits[partition];
                return new AbstractUnaryOutputSourceOperatorNodePushable() {
                    @Override
                    public void initialize() throws HyracksDataException {
                        writer.open();
                        try {
                            TupleWriter tupleWriter = new TupleWriter(ctx,
                                    writer, 1);
                            String text = Rt.readFile(split.getLocalFile()
                                    .getFile());
                            Rt.p("reader" + partition + ": " + text);
                            for (String s : text.split("[ |\t|\r?\n]+")) {
                                tupleWriter.writeString(s);
                                tupleWriter.finishTuple();
                            }
                            tupleWriter.finishFrame();

                        } catch (Exception e) {
                            writer.fail();
                            throw new HyracksDataException(e);
                        } finally {
                            writer.close();
                        }
                    }
                };
            }
        };
        createPartitionConstraint(spec, reader, inSplits);

        IOperatorDescriptor hash = new AbstractOperatorDescriptor(spec, 1, 1) {
            private static final int HASH_BUILD_ACTIVITY_ID = 0;
            private static final int OUTPUT_ACTIVITY_ID = 1;
            {
                recordDescriptors[0] = new RecordDescriptor(
                        new ISerializerDeserializer[2]);
            }

            @Override
            public void contributeActivities(IActivityGraphBuilder builder) {
                IActivity hashActivity = new AbstractActivityNode(new ActivityId(odId,
                        HASH_BUILD_ACTIVITY_ID)) {
                    @Override
                    public IOperatorNodePushable createPushRuntime(
                            final IHyracksTaskContext ctx,
                            final IRecordDescriptorProvider recordDescProvider,
                            final int partition, int nPartitions) {
                        final Object stateId = new TaskId(getActivityId(),
                                partition);
                        return new AbstractUnaryInputSinkOperatorNodePushable() {
                            private HashGroupState state;

                            @Override
                            public void open() throws HyracksDataException {
                                Rt.p("hashBuild" + partition + " open");
                                state = new HashGroupState(ctx
                                        .getJobletContext().getJobId(), stateId);
                            }

                            @Override
                            public void nextFrame(ByteBuffer buffer)
                                    throws HyracksDataException {
                                TupleReader reader = new TupleReader(buffer,
                                        ctx.getFrameSize(), 1);
                                try {
                                    StringBuilder sb = new StringBuilder();
                                    while (reader.nextTuple()) {
                                        reader.seekToField(0);
                                        String s = reader.readString();
                                        Integer count = state.hash.get(s);
                                        if (count == null)
                                            count = 0;
                                        count++;
                                        state.hash.put(s, count);
                                        sb.append(s + ",");
                                    }
                                    Rt.p("hashBuild" + partition
                                            + " nextFrame: " + sb);
                                    reader.close();
                                } catch (IOException e) {
                                    fail();
                                    throw new HyracksDataException(e);
                                }
                            }

                            @Override
                            public void close() throws HyracksDataException {
                                Rt.p("hashBuild" + partition + " close");
                                ctx.setStateObject(state);
                            }

                            @Override
                            public void fail() throws HyracksDataException {
                                throw new HyracksDataException(
                                        "HashGroupOperator is failed.");
                            }
                        };
                    }
                };
                IActivity outputActivity = new AbstractActivityNode(new ActivityId(odId,
                        OUTPUT_ACTIVITY_ID)) {
                    @Override
                    public IOperatorNodePushable createPushRuntime(
                            final IHyracksTaskContext ctx,
                            IRecordDescriptorProvider recordDescProvider,
                            final int partition, int nPartitions) {
                        final Object stateId = new TaskId(new ActivityId(
                                getOperatorId(), HASH_BUILD_ACTIVITY_ID),
                                partition);
                        return new AbstractUnaryOutputSourceOperatorNodePushable() {
                            @Override
                            public void initialize()
                                    throws HyracksDataException {
                                HashGroupState buildState = (HashGroupState) ctx
                                        .getStateObject(stateId);
                                writer.open();
                                TupleWriter tupleWriter = new TupleWriter(ctx,
                                        writer, 2);
                                try {
                                    StringBuilder sb = new StringBuilder();
                                    for (String key : buildState.hash.keySet()) {
                                        tupleWriter.writeString(key);
                                        tupleWriter.finishField();
                                        tupleWriter.writeInt(buildState.hash
                                                .get(key));
                                        tupleWriter.finishField();
                                        tupleWriter.finishTuple();
                                        sb.append(key + ",");
                                    }
                                    tupleWriter.finishFrame();
                                    Rt.p("hashOutput" + partition + ": " + sb);
                                } catch (Exception e) {
                                    writer.fail();
                                    throw new HyracksDataException(e);
                                } finally {
                                    writer.close();
                                }
                            }
                        };
                    }
                };
                builder.addActivity(this, hashActivity);
                builder.addActivity(this, outputActivity);
                builder.addSourceEdge(0, hashActivity, 0);
                builder.addTargetEdge(0, outputActivity, 0);
                builder.addBlockingEdge(hashActivity, outputActivity);
            }
        };

        createPartitionConstraint(spec, hash, outSplits);
        IConnectorDescriptor scanGroupConn = new MToNPartitioningConnectorDescriptor(
                spec, new ITuplePartitionComputerFactory() {
                    @Override
                    public ITuplePartitionComputer createPartitioner() {
                        return new ITuplePartitionComputer() {
                            @Override
                            public int partition(IFrameTupleAccessor accessor,
                                    int tIndex, int nParts)
                                    throws HyracksDataException {
                                try {
                                    TupleReader reader = new TupleReader(
                                            accessor, tIndex);
                                    reader.seekToField(0);
                                    String s = reader.readString();
                                    int targetPartition = (s.charAt(0) - '0')
                                            % nParts;
                                    Rt.p("partition " + s + " -> "
                                            + targetPartition);
                                    return targetPartition;
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                return 0;
                            }
                        };
                    }
                });
        spec.connect(scanGroupConn, reader, 0, hash, 0);

        IOperatorDescriptor writer = new AbstractSingleActivityOperatorDescriptor(
                spec, 1, 0) {
            @Override
            public IOperatorNodePushable createPushRuntime(
                    final IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider,
                    final int partition, int nPartitions)
                    throws HyracksDataException {
                return new AbstractUnaryInputSinkOperatorNodePushable() {
                    private PrintStream out;

                    @Override
                    public void open() throws HyracksDataException {
                        try {
                            Rt.p("writer open");
                            out = new PrintStream(outSplits[partition]
                                    .getLocalFile().getFile());
                        } catch (Exception e) {
                            throw new HyracksDataException(e);
                        }
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer)
                            throws HyracksDataException {
                        try {
                            TupleReader reader = new TupleReader(buffer,
                                    ctx.getFrameSize(), 2);
                            StringBuilder sb = new StringBuilder();
                            while (reader.nextTuple()) {
                                reader.seekToField(0);
                                String word = reader.readString();
                                reader.seekToField(1);
                                int count = reader.readInt();
                                sb.append(word + ",");
                                out.println(count + "\t" + word);
                            }
                            Rt.p("writer" + partition + " nextFrame: " + sb);
                            reader.close();
                        } catch (IOException ex) {
                            throw new HyracksDataException(ex);
                        }
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        Rt.p("writer close");
                        out.close();
                    }
                };
            }
        };
        createPartitionConstraint(spec, writer, outSplits);

        spec.connect(new OneToOneConnectorDescriptor(spec), hash, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }

    public static void main(String[] args) throws Exception {
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

        Client.disableLogging();

        //connect to hyracks
        IHyracksClientConnection hcc = new HyracksConnection("localhost", 3099);

        //update application
        hcc.createApplication("text", null);

        try {
            Rt.write(new File("/tmp/a.txt"), "0a 1b 1c".getBytes());
            Rt.write(new File("/tmp/b.txt"), "0b 1c 1c".getBytes());

            JobSpecification job = createJob(
                    parseFileSplits("NC0:/tmp/a.txt,NC1:/tmp/b.txt"),
                    parseFileSplits("NC0:/tmp/out0.txt,NC1:/tmp/out1.txt"));

            JobId jobId = hcc.startJob("text", job,
                    EnumSet.noneOf(JobFlag.class));
            hcc.waitForCompletion(jobId);

            Rt.np("Output 0:");
            Rt.np(Rt.readFile(new File("/tmp/out0.txt")));
            Rt.np("Output 1:");
            Rt.np(Rt.readFile(new File("/tmp/out1.txt")));
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }
}