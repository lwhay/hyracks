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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;


public class InputFileSplitProviderFactory implements IInputSplitProviderFactory {
    private static final long serialVersionUID = 1L;

    private static MarshalledWritable<Configuration> mConfig = new MarshalledWritable<Configuration>();
    private int partitionId = 0;
//    private static int nPartitions = 0;
    private static List<InputSplit> lis;
//    private static Iterator<InputSplit> iis;

    public InputFileSplitProviderFactory(Job job) throws Exception{
        mConfig.set(job.getConfiguration());
        HadoopHelper helper = new HadoopHelper(mConfig);
        lis = helper.getInputSplits();
//        iis = lis.iterator();
//        nPartitions = lis.size();
    }

    @Override
    public IInputSplitProvider createInputSplitProvider(int partition) throws HyracksDataException { //create provider which make InputSplit[partition]
        partitionId = partition;

        return new IInputSplitProvider() {
            InputSplit split = null;
//            int i = 0;

            @Override
            public InputSplit next() throws HyracksDataException {
//                for(int i = 0; i < partitionId + 1; i++){
//                    split = iis.next();
//                }
                split = lis.get(partitionId);
                return split;
            }
        };
    }

    public int getPartitionCnt(){
        return lis.size();
    }
}