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

package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;


/**
 * High level IMRU job interface. Data passed through are objects.
 * 
 * @author Rui Wang
 * 
 * @param <Model>
 *            data model
 * @param <Data>
 *            data example
 * @param <IntermediateResult>
 *            intermediate result produced by map() and reduce()
 */
public interface IIMRUJob<Model extends Serializable, Data extends Serializable, IntermediateResult extends Serializable>
        extends Serializable {
    /**
     * Frame size must be large enough to store at least one tuple
     */
    public int getCachedDataFrameSize();

    /**
     * Parse input data and output data objects
     */
    public void parse(IMRUContext ctx, InputStream input, DataWriter<Data> output) throws IOException;

    /**
     * For a list of data objects, return one result
     */
    public IntermediateResult map(IMRUContext ctx, Iterator<Data> input, Model model) throws IOException;

    /**
     * Combine multiple results to one result
     */
    public IntermediateResult reduce(IMRUContext ctx, Iterator<IntermediateResult> input) throws IMRUDataException;

    /**
     * update the model using combined result
     */
    public Model update(IMRUContext ctx, Iterator<IntermediateResult> input, Model model) throws IMRUDataException;

    /**
     * Return true to exit loop
     */
    public boolean shouldTerminate(Model model);

}
