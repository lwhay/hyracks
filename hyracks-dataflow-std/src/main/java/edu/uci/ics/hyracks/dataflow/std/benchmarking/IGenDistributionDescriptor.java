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

import java.io.Serializable;

/**
 * The interface for distribution descriptor.
 * 
 * @author jarodwen
 */
public interface IGenDistributionDescriptor extends Serializable {

    /**
     * Retrieve a key based on the given double value.
     * 
     * For any distribution, each possible value within the
     * domain can be assigned with a range r as a subset of [0, 1). 
     * Then the value whose range contains the given double value
     * will be returned.
     * 
     * @param randDouble
     *            A double value within [0, 1)
     * @return
     */
    public int drawKey(double randDouble);

}
