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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class OnlineInputSplitProviderFactory implements IInputSplitProviderFactory<OnlineInputSplitProvider> {
    private static final long serialVersionUID = 1L;

    private MarshalledWritable<Configuration> mConfig;
    
    private Queue<OnlineFileSplit> splits;

    public OnlineInputSplitProviderFactory(MarshalledWritable<Configuration> mConfig) throws HyracksDataException {
        this.mConfig = mConfig;
        this.splits = new LinkedBlockingQueue<OnlineFileSplit>();
        
        try {
        	HadoopHelper helper = new HadoopHelper(mConfig);
            JobContext jCtx = helper.createJobContext();
            // TODO: this.splits.addAll(helper.getInputFormat().getSplits(jCtx));
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public OnlineInputSplitProvider create(int id) throws HyracksDataException {
    	return new OnlineInputSplitProvider(this.splits);
    }
}