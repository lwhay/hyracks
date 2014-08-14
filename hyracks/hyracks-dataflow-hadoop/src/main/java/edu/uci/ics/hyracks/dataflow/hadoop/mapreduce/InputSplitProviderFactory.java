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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.InputFormat;
//import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.conf.*;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class InputSplitProviderFactory implements IInputSplitProviderFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(InputSplitProviderFactory.class.getName());

    private final MarshalledWritable<Configuration> mConfig = new MarshalledWritable<Configuration>();
    private int nPartitions;
    
    public InputSplitProviderFactory(Configuration conf) throws Exception{
        // set nPartitions
    	mConfig.set(conf);
        HadoopHelper helper = new HadoopHelper(mConfig);
        List<InputSplit> lis = null;
        org.apache.hadoop.mapred.InputSplit[] oldlis = null;
        if(helper.getUseNewMapper()){
            lis = helper.getInputSplits();
            nPartitions = lis != null ? lis.size() : 0;
        }
        else{ 
            oldlis = helper.getOldInputSplits();
            nPartitions = oldlis != null ? oldlis.length : 0;
        }
        
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("nPartitions: " + nPartitions);
        }
    }

    @Override
    public IInputSplitProvider createInputSplitProvider(int partition) throws HyracksDataException { //create provider which make InputSplit[partition]
        final int partitionId = partition;
        return new IInputSplitProvider() {
            @Override
            public InputSplit get() throws HyracksDataException {
            	 InputSplit split = null;
                 HadoopHelper helper = new HadoopHelper(mConfig);          
                 List<InputSplit> isl = helper.getInputSplits();
            	
            	if( isl != null && isl.size() != 0 ){
            		split = isl.get(partitionId);
/// DEBUG PRINT ///            		
//System.out.println("[" + Thread.currentThread().getId() + "][InputSplitProviderFactory][get] isl: " + split + ", size: " + isl.size() + ", partitionId: " + partitionId);            		
            	}
            	
                return split;
            }
            
            @Override
            public org.apache.hadoop.mapred.InputSplit getOld() throws HyracksDataException {
                org.apache.hadoop.mapred.InputSplit split = null;
                HadoopHelper helper = new HadoopHelper(mConfig);          
                org.apache.hadoop.mapred.InputSplit[] isl = helper.getOldInputSplits();
                
                if( isl != null && isl.length != 0 ){
/// DEBUG PRINT ///                    
//System.out.println("[" + Thread.currentThread().getId() + "][InputSplitProviderFactory][getOld] isl: " + isl.toString() + ", size: " + isl.length + ", partitionId: " + partitionId);
                    split = isl[partitionId];
                }
                
                return split;
            }
        };
    }

    public int getPartitionCnt(){
        return nPartitions;
    }
}