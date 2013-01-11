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

package edu.uci.ics.hyracks.imru.api2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMRUJobV2<Model, IntermediateResult> extends Serializable {
    /**
     * Return initial model
     */
    public Model initModel();

    /**
     * Frame size must be large enough to store at least one tuple
     */
    public int getCachedDataFrameSize();

    /**
     * Number of fields for each tuple
     */
    public int getFieldCount();

    /**
     * Parse input data and output tuples
     */
    public void parse(IHyracksTaskContext ctx, InputStream input, TupleWriter output) throws IOException;

    /**
     * For each tuple, return one result.
     * Or by using nextTuple(), return one result
     * after processing multiple tuples.
     */
    public IntermediateResult map(IHyracksTaskContext ctx, TupleReader input, Model model, int cachedDataFrameSize)
            throws IOException;

    /**
     * Combine multiple results to one result
     */
    public IntermediateResult reduce(IHyracksTaskContext ctx, Iterator<IntermediateResult> input)
            throws HyracksDataException;

    /**
     * update the model using combined result
     */
    public void update(IHyracksTaskContext ctx, Iterator<IntermediateResult> input, Model model)
            throws HyracksDataException;

    /**
     * Return true to exit loop
     */
    public boolean shouldTerminate(Model model);
}
