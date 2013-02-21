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

package edu.uci.ics.hyracks.imru.trainmerge;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.dataflow.SpreadConnectorDescriptor;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.file.IMRUInputSplitProvider;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.IMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * @author Rui Wang
 * @param <Model>
 */
public class TrainMergeDriver<Model extends Serializable> {
    private final static Logger LOGGER = Logger
            .getLogger(TrainMergeDriver.class.getName());
    private final TrainMergeJob<Model> trainMergejob;
    private Model model;
    private String inputPaths;
    private final IHyracksClientConnection hcc;
    private final IMRUConnection imruConnection;
    private final ConfigurationFactory confFactory;
    private final String app;
    private final UUID id = UUID.randomUUID();

    public String modelFileName;

    public TrainMergeDriver(IHyracksClientConnection hcc,
            IMRUConnection imruConnection, TrainMergeJob<Model> trainMergejob,
            Model initialModel, String inputPaths,
            ConfigurationFactory confFactory, String app) {
        this.trainMergejob = trainMergejob;
        this.model = initialModel;
        this.hcc = hcc;
        this.imruConnection = imruConnection;
        this.inputPaths = inputPaths;
        this.confFactory = confFactory;
        this.app = app;
    }

    public <Model extends Serializable> JobSpecification buildTrainMergeJob(
            String modelFileName, IMRUFileSplit[] inputSplits,
            String[] mapOperatorLocations, String[] mergeLocations)
            throws InterruptedException, IOException {
        JobSpecification job = new JobSpecification();
        //        job.setFrameSize(frameSize);

        Hashtable<String, Integer> hash = new Hashtable<String, Integer>();
        for (int i = 0; i < mergeLocations.length; i++)
            hash.put(mergeLocations[i], i);
        int[] mergerIds = new int[mapOperatorLocations.length];
        for (int i = 0; i < mergerIds.length; i++) {
            mergerIds[i] = hash.get(mapOperatorLocations[i]);
        }

        TrainOD map = new TrainOD(job, trainMergejob, inputSplits, mergerIds,
                mergeLocations.length, imruConnection, id.toString());
        MergeOD fuse = new MergeOD(job, trainMergejob, imruConnection,
                modelFileName, id.toString(), mapOperatorLocations.length);
        job.connect(new SpreadConnectorDescriptor(job, null, null), map, 0,
                fuse, 0);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, map,
                mapOperatorLocations);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(job, fuse,
                mergeLocations);
        job.addRoot(fuse);
        return job;
    }

    public JobStatus run() throws Exception {
        String modelFileName = this.modelFileName == null ? "TM-" + id
                : this.modelFileName;
        IMRUFileSplit[] inputSplits = IMRUInputSplitProvider.getInputSplits(
                inputPaths, confFactory);
        Random random = new Random(id.getLeastSignificantBits());
        String[] trainOperatorLocations = ClusterConfig.setLocationConstraint(
                null, null, inputSplits, random);
        String[] mergeOperatorLocations = new HashSet<String>(
                Arrays.asList(trainOperatorLocations)).toArray(new String[0]);

        LOGGER.info("Starting training id=" + id);
        imruConnection.uploadModel(modelFileName, model);

        LOGGER.info("Distributing initial models");
        JobSpecification spreadjob = IMRUJobFactory.generateModelSpreadJob(
                mergeOperatorLocations, mergeOperatorLocations[0],
                imruConnection, modelFileName, 0, null);
        //        byte[] bs=JavaSerializationUtils.serialize(job);
        //      Rt.p("IMRU job size: "+bs.length);
        JobId spreadjobId = hcc.startJob(app, spreadjob,
                EnumSet.of(JobFlag.PROFILE_RUNTIME));
        //        JobId jobId = hcc.createJob(app, job);
        //        hcc.start(jobId);
        hcc.waitForCompletion(spreadjobId);
        if (hcc.getJobStatus(spreadjobId) == JobStatus.FAILURE)
            return JobStatus.FAILURE;
        LOGGER.info("Starting training");
        long loadStart = System.currentTimeMillis();
        JobSpecification job = buildTrainMergeJob(modelFileName, inputSplits,
                trainOperatorLocations, mergeOperatorLocations);
        JobId jobId = hcc.startJob(app, job,
                EnumSet.of(JobFlag.PROFILE_RUNTIME));
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            String lastStatus = null;

            @Override
            public void run() {
                try {
                    String status = imruConnection.getStatus(id.toString());
                    if (status.length() > 0 && !status.equals(lastStatus)) {
                        Rt.np("Status: " + status);
                        lastStatus = status;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 5000, 5000);
        hcc.waitForCompletion(jobId);
        timer.cancel();
        JobStatus status = hcc.getJobStatus(jobId);
        long loadEnd = System.currentTimeMillis();
        LOGGER.info("Finished training in " + (loadEnd - loadStart)
                + " milliseconds");
        model = (Model) imruConnection.downloadModel(modelFileName);
        return status;
    }

    public Model getModel() {
        return model;
    }

    public UUID getId() {
        return id;
    }
}
