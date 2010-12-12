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
package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCBootstrap;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class CCBootstrap implements ICCBootstrap {
    private ICCApplicationContext appCtx;

    @Override
    public void start() throws Exception {
        final CentralInputSplitQueue queue = new CentralInputSplitQueue();
        appCtx.setDistributedState((Serializable) UnicastRemoteObject.toStub(queue));
        appCtx.addJobLifecycleListener(new IJobLifecycleListener() {
            @Override
            public void notifyJobStart(UUID jobId) {
            }

            @Override
            public void notifyJobFinish(UUID jobId) {
            }

            @Override
            public void notifyJobCreation(UUID jobId, JobSpecification jobSpec) throws HyracksException {
                MarshalledWritable<Configuration> mConfig = (MarshalledWritable<Configuration>) jobSpec
                        .getProperty("jobconf");
                HadoopHelper helper = new HadoopHelper(mConfig);
                List<InputSplit> iSplits = helper.getInputSplits();
                List<OnlineFileSplit> splits = new ArrayList<OnlineFileSplit>();

                int counter = 0;
                for (InputSplit is : iSplits) {
                    FileSplit fs = (FileSplit) is;
                    try {
                        splits.add(new OnlineFileSplit(counter++, fs.getPath(), fs.getStart(), fs.getLength(), fs
                                .getLocations(), 0));
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }

                // TODO: Permute splits
                queue.addSplits(jobId, splits);
            }
        });
    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public void setApplicationContext(ICCApplicationContext appCtx) {
        this.appCtx = appCtx;
    }
}