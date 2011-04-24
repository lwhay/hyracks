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

import java.util.Random;

/**
 * Random distribution descriptor. 
 * 
 * Given a randomly generated double value in the range of [0, 1), this
 * descriptor will return a key for the given range [0, cardinality). 
 * 
 * For example, for integer values [0, 2], the following behaviors are expected:
 * - if the given value is in [0, 1/3), then return 0;
 * - if the given value is in [1/3, 2/3), then return 1;
 * - if the given value is in [2/3, 1), then return 2.
 * 
 * @author jarodwen
 *
 */
public class RandomDistributionDescriptor implements IGenDistributionDescriptor {

    private static final long serialVersionUID = 1L;
    private final int cardinality;
    
    public RandomDistributionDescriptor(){
        this.cardinality = Integer.MAX_VALUE;
    }
    
    public RandomDistributionDescriptor(int cardinality) throws Exception{
        if(cardinality < 0)
            throw new Exception("The cardinality cannot be negative.");
        this.cardinality = cardinality;
    }
    
    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.examples.benchmarking.app.utils.IGenDistributionDescriptor#drawKey(double)
     */
    public int drawKey(double randDouble) {
        return (int)(randDouble * cardinality);
    }
    
    public static void main(String[] args) throws Exception{
        RandomDistributionDescriptor randDist = new RandomDistributionDescriptor();
        Random rand = new Random();
        for(int i = 0; i < 100; i ++){
            System.out.println(randDist.drawKey(rand.nextDouble()));
        }
        randDist = new RandomDistributionDescriptor(1000000);
        for(int i = 0; i < 100; i ++){
            System.out.println(randDist.drawKey(rand.nextDouble()));
        }
    }

}
