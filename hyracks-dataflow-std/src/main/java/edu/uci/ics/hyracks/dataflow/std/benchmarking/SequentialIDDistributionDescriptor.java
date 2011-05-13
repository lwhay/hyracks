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


/**
 * @author jarodwen
 *
 */
public class SequentialIDDistributionDescriptor implements IGenDistributionDescriptor {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private static int index;
    
    public SequentialIDDistributionDescriptor(){
        
    }
    
    public SequentialIDDistributionDescriptor(int idx) {
        index = idx;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.benchmarking.IGenDistributionDescriptor#drawKey(double)
     */
    @Override
    public int drawKey(double randDouble) {
        return index++;
    }

}
