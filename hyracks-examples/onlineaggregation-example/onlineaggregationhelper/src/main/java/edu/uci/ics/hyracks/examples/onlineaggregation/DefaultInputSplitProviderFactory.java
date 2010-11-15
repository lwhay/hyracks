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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DefaultInputSplitProviderFactory implements IInputSplitProviderFactory {
    private MarshalledWritable<Configuration> mConfig;

    public DefaultInputSplitProviderFactory(MarshalledWritable<Configuration> mConfig) {
        this.mConfig = mConfig;
    }

    @Override
    public IInputSplitProvider createInputSplitProvider() throws HyracksDataException {
        HadoopHelper helper = new HadoopHelper(mConfig);
        final List<InputSplit> splits;
        try {
            JobContext jCtx = new JobContext(mConfig.get(), null);
            splits = helper.getInputFormat().getSplits(jCtx);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        return new IInputSplitProvider() {
            private int counter = -1;
            private boolean done = false;
            private InputSplit split;
            private int blockId;

            @Override
            public boolean next(int partition, int nPartitions) {
                if (done) {
                    return false;
                }
                ++counter;
                blockId = counter * nPartitions + partition;
                if (blockId >= splits.size()) {
                    done = true;
                    return false;
                }
                split = splits.get(blockId);
                return true;
            }

            @Override
            public InputSplit getInputSplit() {
                return split;
            }

            @Override
            public int getBlockId() {
                return blockId;
            }
        };
    }
}