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
package edu.uci.ics.hyracks.examples.benchmarking.helpers;

/**
 * Interface for primitive data generator.
 *
 */
public interface IDataGenerator<T> {
    /**
     * Generate a data, randomly.
     * 
     * @return
     */
    public T generate();
    
    /**
     * Generate a data for the given key value. 
     * 
     * This is used to control the data generated.
     * 
     * @param key
     * @return
     */
    public T generate(long key);
}
