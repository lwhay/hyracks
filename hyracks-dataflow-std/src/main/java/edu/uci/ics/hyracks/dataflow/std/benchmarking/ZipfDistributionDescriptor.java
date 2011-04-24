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
 * An implementation of the Zipf's Law (<a>http://en.wikipedia.org/wiki/Zipf's_law</a>) using
 * {@link IGenDistributionDescriptor}. For the given range of possible values in [min, max], this
 * implementation will return a value based on the probability distribution.
 * 
 * For example, for integer values [0, 2] and skew factor 1, the following behaviors are expected:
 * - if the given value is in [0, 6/11), then return 0;
 * - if the given value is in [6/11, 9/11), then return 1;
 * - if the given value is in [9/11, 1), then return 2.
 * 
 * @author jarodwen
 *
 */
public class ZipfDistributionDescriptor implements IGenDistributionDescriptor {

    private static final long serialVersionUID = 1L;
    private final int cardinality;
    private final double skew;
    
    private final double denominator;
    
    public ZipfDistributionDescriptor(int cardinality, double skew){
        this.cardinality = cardinality;
        this.skew = skew;
        double denom = 0;
        for(int l = 1; l <= cardinality; l++){
            denom += 1/Math.pow(l, skew);
        }
        this.denominator = denom;
    }
    
    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.examples.benchmarking.app.utils.IGenDistributionDescriptor#drawKey(double)
     */
    public int drawKey(double randDouble) {
        int rtn = 0;
        double accum = 0.0;
        // FIXME Nicer way to do this?
        while(accum < randDouble && rtn <= cardinality){
            accum += (1/Math.pow(rtn + 1, skew)) / denominator;
            rtn++;
        }
        return rtn;
    }
    
    public static void main(String[] args){
        ZipfDistributionDescriptor randDist = new ZipfDistributionDescriptor(10000, 1);
        Random rand = new Random();
        for(int i = 0; i < 10000; i ++){
            System.out.println(randDist.drawKey(rand.nextDouble()));
        }
    }

}
