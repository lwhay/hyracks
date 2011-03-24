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

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;

/**
 * The interface for data generator.
 * 
 * This interface is used to define the default behavior
 * supported by generators of different data types. The
 * following behaviors are included and should be guaranteed
 * in the implementation:
 * 
 * - generate() function should return a value of this
 * data type in a (pseudo) random way;
 * 
 * - generate(key) function should return a value of this
 * data type, and different calls of this function with the
 * same key should always return the same value.
 * 
 * This will isolate the implementation of data generating
 * from the control of the data distribution. For more info
 * on this, see {@link IGenDistributionDescriptor}.
 * 
 * @author jarodwen
 *
 */
public interface ITypeGenerator<T> extends Serializable {
    
    /**
     * Generate a data with type T.
     * 
     * @return
     */
    public T generate();
    
    /**
     * Generate a data with type T, based on the input key. 
     * This function guarantees that the same generator will
     * generate the same data when the keys are same.
     * 
     * @param key
     * @return
     */
    public T generate(int key);
    
    /**
     * Reset the generator.
     */
    public void reset();
    
    /**
     * Return the serializer/deserializer for this data type.
     */
    public ISerializerDeserializer<T> getSeDerInstance();
    
}
