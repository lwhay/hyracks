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
package edu.uci.ics.hyracks.dataflow.std.benchmarking;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

/**
 * @author jarodwen
 *
 */
public class IntegerRangePartitionedGenerator implements ITypeGenerator<Integer> {

    private static final long serialVersionUID = 1L;
    private final Random rand;
    private int randSeed;
    
    private int partitionID = -1;
    
    private String[] nodes;  
    
    private final int max;
    
    private boolean isInitialized = false;
    
    private int partitionMin, partitionMax;

    public IntegerRangePartitionedGenerator(int max, int randSeed, String[] nodes) throws UnknownHostException {
        this.randSeed = randSeed;
        this.rand = new Random(randSeed);
        this.nodes = nodes;
        this.max = max;
        this.partitionMin = 0;
        this.partitionMax = max;
    }
    
    private void initPartition(){
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for(int i = 0; i < nodes.length; i++){
            if(hostname.contains(nodes[i])){
                partitionID = i;
                break;
            }
        }
        if(partitionID < 0){
            partitionMin = 0;
            partitionMax = max;
        } else {
            partitionMin = max / nodes.length * partitionID;
            partitionMax = max / nodes.length * (partitionID + 1);
        }
        isInitialized = true;
    }
    
    @Override
    public Integer generate() {
        if(!isInitialized){
            initPartition();
        }
        return rand.nextInt(partitionMax - partitionMin) + partitionMin;
    }

    @Override
    public Integer generate(int key) {
        if(!isInitialized){
            initPartition();
        }
        return (key + randSeed) % (partitionMax - partitionMin) + partitionMin;
    }

    @Override
    public void reset() {
        randSeed = rand.nextInt();
        rand.setSeed(randSeed);
        this.isInitialized = false;
    }

    @Override
    public ISerializerDeserializer<Integer> getSeDerInstance() {
        return IntegerSerializerDeserializer.INSTANCE;
    }

}
